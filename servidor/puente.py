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
  Binario (datos del juego):
    cliente → [destino int32 LE][canal u8][modo u8][carga]    destino 0 = todos, <0 = todos menos -destino
    cliente ← [origen  int32 LE][canal u8][modo u8][carga]

Arrancar:  python puente.py            (puerto: variable PORT, por defecto 8080)
"""

import asyncio
import json
import os
import random
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


async def atender(ws) -> None:
    sala: Sala | None = None
    mi_id = 0
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
            tipo = datos.get("t")
            if tipo == "crear" and sala is None:
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
        return conexion.respond(200, f"Zona Muerta: puente activo · {len(salas)} salas\n")
    return None


async def main() -> None:
    puerto = int(os.environ.get("PORT", "8080"))
    async with serve(atender, "0.0.0.0", puerto, process_request=salud, max_size=2**20,
                     ping_interval=20, ping_timeout=30):
        print(f"Puente escuchando en el puerto {puerto}")
        await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    asyncio.run(main())
