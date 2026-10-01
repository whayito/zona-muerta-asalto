"""Avisos con el juego cerrado (Firebase Cloud Messaging) y almacén de tokens (Firestore).

Usa la cuenta de servicio de la variable de entorno FIREBASE_CREDENCIALES (el JSON entero).
Esa clave solo existe en el servidor: nunca va en el juego ni en el repositorio.

  · Firestore, colección «tokens»: un documento por código de amigo (en minúsculas) con
    {token, codigo, uid, actualizado}. Solo el piloto que lo creó (mismo uid) puede cambiarlo.
  · FCM API HTTP v1: notificación «NOMBRE te invita a su sala» con los datos {sala, de, nombre}
    (Android los pasa al juego al tocar el aviso).

Sin FIREBASE_CREDENCIALES todo queda desactivado y el resto del puente funciona igual.
Con FIREBASE_FALSO=1 (pruebas locales) se usa un almacén en memoria y los envíos solo se registran.
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

AMBITOS = ["https://www.googleapis.com/auth/firebase.messaging", "https://www.googleapis.com/auth/datastore"]


class Avisos:
    def __init__(self):
        self.falso = os.environ.get("FIREBASE_FALSO", "") == "1"
        self.activo = False
        self._cred = None
        self.proyecto = ""
        self._memoria: dict[str, dict] = {}
        self.enviados: list[dict] = []          # solo en modo falso (pruebas)
        if self.falso:
            self.activo = True
            self.proyecto = "falso"
            print("Avisos: modo falso (pruebas)", flush=True)
            return
        texto = os.environ.get("FIREBASE_CREDENCIALES", "").strip()
        if not texto:
            print("Avisos: sin FIREBASE_CREDENCIALES, desactivados", flush=True)
            return
        try:
            from google.oauth2 import service_account
            info = json.loads(texto)
            self._cred = service_account.Credentials.from_service_account_info(info, scopes=AMBITOS)
            self.proyecto = info.get("project_id", "")
            self.activo = bool(self.proyecto)
            print(f"Avisos: activos (proyecto {self.proyecto})", flush=True)
        except Exception as e:  # sin romper el puente por esto
            print("Avisos: credenciales no válidas:", type(e).__name__, flush=True)

    # ─── Autenticación y peticiones ───────────────────────────────────────────

    def _token_acceso(self) -> str:
        from google.auth.transport.requests import Request
        if not self._cred.valid:
            self._cred.refresh(Request())
        return self._cred.token

    def _pedir(self, metodo: str, url: str, cuerpo: dict | None = None) -> tuple[int, dict]:
        datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
        pet = urllib.request.Request(url, data=datos, method=metodo, headers={
            "Authorization": "Bearer " + self._token_acceso(), "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(pet, timeout=15) as r:
                texto = r.read().decode() or "{}"
                return r.status, json.loads(texto)
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read().decode() or "{}")
            except ValueError:
                return e.code, {}

    def _url_doc(self, codigo: str) -> str:
        doc = urllib.parse.quote(codigo.lower(), safe="")
        return (f"https://firestore.googleapis.com/v1/projects/{self.proyecto}"
                f"/databases/(default)/documents/tokens/{doc}")

    # ─── Almacén de tokens ────────────────────────────────────────────────────

    def leer(self, codigo: str) -> dict | None:
        if self.falso:
            return self._memoria.get(codigo.lower())
        estado, d = self._pedir("GET", self._url_doc(codigo))
        if estado != 200:
            return None
        campos = d.get("fields", {})
        return {k: v.get("stringValue", "") for k, v in campos.items()}

    def guardar(self, codigo: str, uid: str, token: str) -> str:
        """Guarda (o borra, si token = "") el token de un código. Devuelve "ok" o el motivo."""
        actual = self.leer(codigo)
        if actual is not None and actual.get("uid") and actual.get("uid") != uid:
            return "otro_piloto"          # ese código es de otro piloto: no se toca
        if self.falso:
            if token:
                self._memoria[codigo.lower()] = {"token": token, "codigo": codigo, "uid": uid}
            else:
                self._memoria.pop(codigo.lower(), None)
            return "ok"
        if not token:
            estado, _ = self._pedir("DELETE", self._url_doc(codigo))
            return "ok" if estado in (200, 404) else f"error {estado}"
        campos = {"token": token, "codigo": codigo, "uid": uid, "actualizado": str(int(time.time()))}
        estado, _ = self._pedir("PATCH", self._url_doc(codigo),
                                {"fields": {k: {"stringValue": v} for k, v in campos.items()}})
        return "ok" if estado == 200 else f"error {estado}"

    # ─── Envío ────────────────────────────────────────────────────────────────

    def enviar_invitacion(self, codigo_destino: str, de: str, nombre: str, sala: str) -> str:
        """Envía el aviso de invitación al móvil del destinatario. Devuelve "enviado",
        "sin_token" o el motivo del fallo."""
        doc = self.leer(codigo_destino)
        if not doc or not doc.get("token"):
            return "sin_token"
        quien = nombre or de
        mensaje = {
            "token": doc["token"],
            "notification": {"title": f"{quien} te invita a su sala",
                             "body": f"Sala {sala} · toca para unirte en Space Empire: The Dead Zone"},
            "data": {"tipo": "invitacion", "sala": sala, "de": de, "nombre": nombre},
            "android": {"priority": "high", "ttl": "600s",
                        "notification": {"tag": "invitacion"}},
        }
        if self.falso:
            self.enviados.append(mensaje)
            print("Avisos (falso): enviado", json.dumps(mensaje, ensure_ascii=False), flush=True)
            return "enviado"
        estado, d = self._pedir("POST", f"https://fcm.googleapis.com/v1/projects/{self.proyecto}/messages:send",
                                {"message": mensaje})
        if estado == 200:
            return "enviado"
        error = str(d.get("error", {}).get("status", ""))
        if estado == 404 or error in ("NOT_FOUND", "UNREGISTERED"):
            # El token ya no vale (app desinstalada o datos borrados): se olvida.
            self._pedir("DELETE", self._url_doc(codigo_destino))
        print(f"Avisos: FCM respondió {estado} {error}", flush=True)
        return f"error {estado} {error}".strip()
