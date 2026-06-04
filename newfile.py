import asyncio
import websockets
import json
from datetime import datetime

# ---------------- CONFIGURACION ----------------

PUERTO = 8765
clientes = {}  # {websocket: nombre}


# ---------------- BROADCAST ----------------

async def broadcast(mensaje, remitente=None):
    desconectados = []
    for cliente in clientes:
        try:
            await cliente.send(json.dumps(mensaje))
        except:
            desconectados.append(cliente)

    for c in desconectados:
        clientes.pop(c, None)


# ---------------- MANEJAR CLIENTE ----------------

async def manejar_cliente(websocket):
    nombre = None
    try:
        # Primer mensaje: registro con nombre
        datos = await websocket.recv()
        info = json.loads(datos)
        nombre = info.get("nombre", "Desconocido")
        clientes[websocket] = nombre

        print(f"✅ {nombre} conectado. Total: {len(clientes)}")

        # Notificar a todos
        await broadcast({
            "tipo": "sistema",
            "texto": f"{nombre} se conectó 👋",
            "hora": datetime.now().strftime("%H:%M")
        })

        # Escuchar mensajes
        async for datos in websocket:
            mensaje = json.loads(datos)
            mensaje["hora"] = datetime.now().strftime("%H:%M")
            print(f"[{mensaje['hora']}] {nombre}: {mensaje.get('texto', '')}")
            await broadcast(mensaje)

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        if websocket in clientes:
            clientes.pop(websocket)
        if nombre:
            print(f"❌ {nombre} desconectado.")
            await broadcast({
                "tipo": "sistema",
                "texto": f"{nombre} se desconectó",
                "hora": datetime.now().strftime("%H:%M")
            })


# ---------------- MAIN ----------------

async def main():
    print(f"🚀 Servidor iniciado en puerto {PUERTO}")
    print(f"📡 Esperando conexiones...")
    async with websockets.serve(manejar_cliente, "0.0.0.0", PUERTO):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
