"""Servidor puente de Zona Muerta: Asalto.

Anfitrión y amigos se conectan HACIA este servidor (WebSocket), así que ningún router tiene
que abrir puertos: funciona con datos móviles, CGNAT o cualquier wifi. El servidor no sabe
nada del juego: solo crea salas con un código y reenvía los paquetes entre sus miembros.

Protocolo
  Texto (JSON, control):
    cliente → {"t": "crear", "version": "1.5"}             ← {"t": "sala", "codigo": "KXQM", "id": 1}
    cliente → {"t": "unir", "codigo": "KXQM", "version"}   ← {"t": "bienvenido", "id": N, "pares": [1, ...]}
                                                           ← {"t": "version", "anfitrion": "1.5"}  (distinta versión)
                                                           ← {"t": "error", "texto": "..."}
    anfitrión → {"t": "echar", "id": N}
    cualquiera → {"t": "informe", "titulo": "...", "texto": "..."}  ← {"t": "informe_ok", "donde": "..."}
    a todos   ← {"t": "entra", "id": N} · {"t": "sale", "id": N} · {"t": "cerrada"}
  Presencia y amigos (desde la 0.2.5, en una conexión aparte; las salas no cambian):
    cliente → {"t": "hola", "id": "uuid", "codigo": "NOMBRE#1234", "nombre", "version"}
                                                           ← {"t": "hola_ok"} · {"t": "error_amigos", "texto"}
    cliente → {"t": "estado", "estado": "menu" | "partida" | "sala:KXQM"}
    cliente → {"t": "amigos?", "codigos": [...]}           ← {"t": "amigos", "estados": {codigo: estado | "desconectado"}}
    cliente → {"t": "existe?", "codigo": "..."}            ← {"t": "existe", "codigo", "si": bool}
    cliente → {"t": "invitar", "a": "NOMBRE#1234", "sala": "KXQM"}
                                                           ← {"t": "invitado", "a", "ok": bool, "texto"}
    destinatario ← {"t": "invitacion", "de": "NOMBRE#1234", "nombre", "sala"}
  Solo en memoria: quién está conectado ahora y con qué código. Límite de mensajes por conexión.
  Binario (datos del juego):
    cliente → [destino int32 LE][canal u8][modo u8][carga]    destino 0 = todos, <0 = todos menos -destino
    cliente ← [origen  int32 LE][canal u8][modo u8][carga]

Arrancar:  python puente.py            (puerto: variable PORT, por defecto 8080)
"""

import asyncio
import json
import os
import random
import re
import struct
import time
import urllib.request

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

ALFABETO = "ABCDEFGHJKLMNPQRSTUVWXYZ"
# Buzón de informes: avisos (issues) en un repositorio privado de GitHub. El token vive solo
# aquí, en el servidor (variable de entorno), nunca dentro del juego.
TOKEN_INFORMES = os.environ.get("GITHUB_TOKEN", "")
REPO_INFORMES = os.environ.get("REPO_INFORMES", "whayito/zona-muerta-informes")
ultimos_informes: list[float] = []
MAX_JUGADORES = 4
salas: dict[str, "Sala"] = {}

# Presencia (amigos): código de amigo → conexión que lo tiene ahora. Solo en memoria.
CODIGO_AMIGO = re.compile(r"^[0-9A-Za-zÁÉÍÓÚÜÑáéíóúüñ_\- ]{1,16}#[0-9]{4}$")
CODIGO_SALA = re.compile(r"^[A-Z]{4}$")
MAX_INVITACIONES_MIN = 10          # por conexión
MAX_MENSAJES_10S = 40              # mensajes de control por conexión cada 10 s
MAX_CONSULTA = 100                 # códigos por consulta de amigos
presentes: dict[str, "Presente"] = {}


class Presente:
    def __init__(self, ws, uid: str, codigo: str, nombre: str, version: str):
        self.ws = ws
        self.uid = uid
        self.codigo = codigo
        self.nombre = nombre
        self.version = version
        self.estado = "menu"


class Sala:
    def __init__(self, codigo: str, anfitrion, version: str):
        self.codigo = codigo
        self.version = version
        self.miembros = {1: anfitrion}   # id → conexión
        self.creada = time.time()

    def otro_id(self) -> int:
        while True:
            n = random.randint(2, 2_000_000_000)
            if n not in self.miembros:
                return n


def nuevo_codigo() -> str:
    while True:
        c = "".join(random.choice(ALFABETO) for _ in range(4))
        if c not in salas:
            return c


async def enviar_json(ws, datos: dict) -> None:
    try:
        await ws.send(json.dumps(datos))
    except ConnectionClosed:
        pass


def normalizar_codigo(texto) -> str:
    """«nombre#1234» → como se guarda: el nombre tal cual (recortado) y 4 cifras."""
    t = str(texto or "").strip()
    return t if CODIGO_AMIGO.match(t) else ""


def estado_valido(texto) -> str:
    t = str(texto or "")
    if t in ("menu", "partida"):
        return t
    if t.startswith("sala:") and CODIGO_SALA.match(t[5:]):
        return t
    return "menu"


async def amigos(ws, datos: dict, yo: "Presente | None", avisos: dict) -> "Presente | None":
    """Mensajes de presencia y amigos. Devuelve el Presente de esta conexión (o None)."""
    tipo = datos.get("t")
    ahora = time.time()
    if tipo == "hola":
        codigo = normalizar_codigo(datos.get("codigo"))
        uid = str(datos.get("id", ""))[:64]
        if not codigo or not uid:
            await enviar_json(ws, {"t": "error_amigos", "texto": "Código de amigo no válido."})
            return yo
        if yo is not None and presentes.get(yo.codigo.lower()) is yo:
            presentes.pop(yo.codigo.lower(), None)
        otro = presentes.get(codigo.lower())
        if otro is not None and otro.uid != uid:
            # Otro piloto distinto ya usa ese código ahora mismo: no se le quita.
            await enviar_json(ws, {"t": "error_amigos", "texto": "Ese código de amigo ya está en uso."})
            return yo
        yo = Presente(ws, uid, codigo, str(datos.get("nombre", ""))[:16], str(datos.get("version", ""))[:16])
        presentes[codigo.lower()] = yo
        await enviar_json(ws, {"t": "hola_ok"})
        return yo
    if yo is None:
        return yo
    if tipo == "estado":
        yo.estado = estado_valido(datos.get("estado"))
    elif tipo == "amigos?":
        codigos = datos.get("codigos", [])
        if not isinstance(codigos, list):
            codigos = []
        estados = {}
        for c in codigos[:MAX_CONSULTA]:
            c = normalizar_codigo(c)
            if c:
                p = presentes.get(c.lower())
                estados[c] = p.estado if p is not None else "desconectado"
        await enviar_json(ws, {"t": "amigos", "estados": estados})
    elif tipo == "existe?":
        c = normalizar_codigo(datos.get("codigo"))
        await enviar_json(ws, {"t": "existe", "codigo": c, "si": bool(c) and c.lower() in presentes})
    elif tipo == "invitar":
        destino = normalizar_codigo(datos.get("a"))
        sala = str(datos.get("sala", "")).strip().upper()
        avisos["invitaciones"] = [t for t in avisos.get("invitaciones", []) if ahora - t < 60]
        if len(avisos["invitaciones"]) >= MAX_INVITACIONES_MIN:
            await enviar_json(ws, {"t": "invitado", "a": destino, "ok": False, "texto": "Demasiadas invitaciones: espera un minuto."})
            return yo
        p = presentes.get(destino.lower()) if destino else None
        if p is None or not CODIGO_SALA.match(sala) or sala not in salas:
            await enviar_json(ws, {"t": "invitado", "a": destino, "ok": False, "texto": "No está conectado o la sala no existe."})
            return yo
        avisos["invitaciones"].append(ahora)
        await enviar_json(p.ws, {"t": "invitacion", "de": yo.codigo, "nombre": yo.nombre, "sala": sala})
        await enviar_json(ws, {"t": "invitado", "a": destino, "ok": True, "texto": ""})
    return yo


async def atender(ws) -> None:
    sala: Sala | None = None
    mi_id = 0
    yo: Presente | None = None
    avisos: dict = {}
    control: list[float] = []
    try:
        async for mensaje in ws:
            if isinstance(mensaje, bytes):
                if sala is None or len(mensaje) < 6:
                    continue
                destino, canal, modo = struct.unpack_from("<iBB", mensaje, 0)
                paquete = struct.pack("<iBB", mi_id, canal, modo) + mensaje[6:]
                if destino > 0:
                    otro = sala.miembros.get(destino)
                    objetivos = [otro] if otro is not None else []
                else:
                    excluido = -destino if destino < 0 else 0
                    objetivos = [c for i, c in sala.miembros.items() if i != mi_id and i != excluido]
                for c in objetivos:
                    try:
                        await c.send(paquete)
                    except ConnectionClosed:
                        pass
                continue

            try:
                datos = json.loads(mensaje)
            except ValueError:
                continue
            if not isinstance(datos, dict):
                continue
            # Anti-abuso: tope de mensajes de control por conexión.
            ahora = time.time()
            control[:] = [t for t in control if ahora - t < 10]
            if len(control) >= MAX_MENSAJES_10S:
                continue
            control.append(ahora)
            tipo = datos.get("t")
            if tipo in ("hola", "estado", "amigos?", "existe?", "invitar"):
                yo = await amigos(ws, datos, yo, avisos)
            elif tipo == "crear" and sala is None:
                codigo = nuevo_codigo()
                sala = Sala(codigo, ws, str(datos.get("version", "")))
                salas[codigo] = sala
                mi_id = 1
                await enviar_json(ws, {"t": "sala", "codigo": codigo, "id": 1})
                print(f"sala {codigo} creada ({len(salas)} activas)")
            elif tipo == "unir" and sala is None:
                codigo = str(datos.get("codigo", "")).strip().upper()
                s = salas.get(codigo)
                if s is None:
                    await enviar_json(ws, {"t": "error", "texto": f"No existe la sala {codigo}. Revisa el código."})
                    continue
                if str(datos.get("version", "")) != s.version:
                    await enviar_json(ws, {"t": "version", "anfitrion": s.version})
                    continue
                if len(s.miembros) >= MAX_JUGADORES:
                    await enviar_json(ws, {"t": "error", "texto": "La sala está llena (4 pilotos)."})
                    continue
                mi_id = s.otro_id()
                pares = list(s.miembros.keys())
                s.miembros[mi_id] = ws
                sala = s
                await enviar_json(ws, {"t": "bienvenido", "id": mi_id, "pares": pares})
                for i, c in list(s.miembros.items()):
                    if i != mi_id:
                        await enviar_json(c, {"t": "entra", "id": mi_id})
            elif tipo == "informe":
                donde = await guardar_informe(str(datos.get("titulo", "Informe"))[:200], str(datos.get("texto", ""))[:60000])
                await enviar_json(ws, {"t": "informe_ok", "donde": donde})
            elif tipo == "echar" and sala is not None and mi_id == 1:
                otro = sala.miembros.get(int(datos.get("id", 0)))
                if otro is not None:
                    await otro.close()
    except ConnectionClosed:
        pass
    finally:
        if sala is not None:
            await salir(sala, mi_id)
        if yo is not None and presentes.get(yo.codigo.lower()) is yo:
            presentes.pop(yo.codigo.lower(), None)


async def salir(sala: Sala, mi_id: int) -> None:
    sala.miembros.pop(mi_id, None)
    if mi_id == 1:
        # Sin anfitrión no hay partida: se cierra la sala para todos.
        salas.pop(sala.codigo, None)
        for c in list(sala.miembros.values()):
            await enviar_json(c, {"t": "cerrada"})
            await c.close()
        sala.miembros.clear()
        print(f"sala {sala.codigo} cerrada ({len(salas)} activas)")
    else:
        for c in list(sala.miembros.values()):
            await enviar_json(c, {"t": "sale", "id": mi_id})


async def guardar_informe(titulo: str, texto: str) -> str:
    """Crea un aviso en el buzón privado. Sin token, al menos queda en el registro del servidor."""
    ahora = time.time()
    ultimos_informes[:] = [t for t in ultimos_informes if ahora - t < 3600]
    if len(ultimos_informes) >= 30:
        return "limite"
    ultimos_informes.append(ahora)
    print(f"=== INFORME: {titulo}\n{texto}\n=== FIN INFORME", flush=True)
    if not TOKEN_INFORMES:
        return "registro"

    def crear() -> str:
        cuerpo = json.dumps({"title": titulo, "body": "```\n" + texto + "\n```", "labels": ["informe"]}).encode()
        pet = urllib.request.Request(
            f"https://api.github.com/repos/{REPO_INFORMES}/issues", data=cuerpo, method="POST",
            headers={"Authorization": f"Bearer {TOKEN_INFORMES}", "Accept": "application/vnd.github+json",
                     "User-Agent": "zona-muerta-puente"})
        with urllib.request.urlopen(pet, timeout=15) as r:
            return str(json.loads(r.read()).get("number", "?"))

    try:
        return "github #" + await asyncio.to_thread(crear)
    except Exception as e:  # sin romper el puente por un informe
        print("No se pudo crear el aviso en GitHub:", e, flush=True)
        return "registro"


def salud(conexion, peticion):
    """Responde a las visitas normales (el alojamiento comprueba que el servidor vive)."""
    if peticion.headers.get("Upgrade", "").lower() != "websocket":
        return conexion.respond(200, f"Zona Muerta: puente activo · {len(salas)} salas · {len(presentes)} pilotos\n")
    return None


async def main() -> None:
    puerto = int(os.environ.get("PORT", "8080"))
    async with serve(atender, "0.0.0.0", puerto, process_request=salud, max_size=2**20,
                     ping_interval=20, ping_timeout=30):
        print(f"Puente escuchando en el puerto {puerto}")
        await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    asyncio.run(main())
