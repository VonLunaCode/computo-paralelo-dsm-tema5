#!/usr/bin/env python3
"""DSM mínima basada en páginas (gestor central, escritura-invalidación).

Solo usa la biblioteca estándar de Python 3.8+ (funciona en Linux y Windows).

Roles:
  maestro     guarda el directorio (dueño y copias de cada página) y los datos.
  trabajador  se conecta al maestro, cachea páginas y corre los experimentos.

Uso en un clúster de laptops (misma red local):
  laptop maestra:      python dsm_demo.py maestro --trabajadores 2
  cada trabajadora:    python dsm_demo.py trabajador --maestro 192.168.1.10 --id 1
                       python dsm_demo.py trabajador --maestro 192.168.1.10 --id 2
Todo en una sola máquina (prueba):
  python dsm_demo.py local --trabajadores 2
"""
import argparse
import base64
import json
import os
import queue
import socket
import struct
import subprocess
import sys
import threading
import time

PAGINA = 4096          # bytes por página
NUM_PAGINAS = 128      # espacio compartido: 512 KiB
LECTURA, ESCRITURA = "R", "W"


# ---------------------------------------------------------------- red
def enviar(sock, msg, candado):
    datos = json.dumps(msg).encode()
    with candado:
        sock.sendall(struct.pack("!I", len(datos)) + datos)


def recibir(sock):
    cab = _exacto(sock, 4)
    if cab is None:
        return None
    (n,) = struct.unpack("!I", cab)
    cuerpo = _exacto(sock, n)
    return None if cuerpo is None else json.loads(cuerpo)


def _exacto(sock, n):
    buf = b""
    while len(buf) < n:
        trozo = sock.recv(n - len(buf))
        if not trozo:
            return None
        buf += trozo
    return buf


def b64(b):
    return base64.b64encode(bytes(b)).decode()


def unb64(s):
    return bytearray(base64.b64decode(s))


# ---------------------------------------------------------------- maestro
class Maestro:
    """Gestor central: directorio + copia canónica de cada página.

    Las peticiones se atienden en UN solo hilo (cola), así el directorio no
    necesita candados y las respuestas (ACK/devolución) nunca se bloquean.
    """

    def __init__(self, n_trab):
        self.n_trab = n_trab
        self.datos = [bytearray(PAGINA) for _ in range(NUM_PAGINAS)]
        self.dueno = [None] * NUM_PAGINAS          # trabajador con modo W
        self.copias = [set() for _ in range(NUM_PAGINAS)]  # con modo R
        self.conex = {}                            # id -> (sock, candado)
        self.cola = queue.Queue()
        self.pendientes = {}                       # clave -> (Event, [resp])
        self.barrera = []
        self.resultados = {}
        self.listo = threading.Event()
        self.msgs = 0

    # -- E/S
    def a(self, tid, msg):
        sock, cand = self.conex[tid]
        self.msgs += 1
        enviar(sock, msg, cand)

    def esperar(self, tid, msg, clave):
        ev, caja = threading.Event(), []
        self.pendientes[clave] = (ev, caja)
        self.a(tid, msg)
        ev.wait()
        return caja[0]

    def lector(self, tid, sock):
        while True:
            m = recibir(sock)
            if m is None:
                return
            if m["t"] in ("ACK", "DEVUELVE"):
                ev, caja = self.pendientes.pop((tid, m["t"], m["p"]))
                caja.append(m)
                ev.set()
            else:
                self.cola.put((tid, m))

    # -- protocolo
    def recuperar(self, p, invalidar):
        """Trae la última versión de p desde su dueño (si lo hay)."""
        d = self.dueno[p]
        if d is None:
            return
        r = self.esperar(d, {"t": "RECALL", "p": p, "inv": invalidar},
                         (d, "DEVUELVE", p))
        self.datos[p] = unb64(r["d"])
        self.dueno[p] = None
        if not invalidar:
            self.copias[p].add(d)

    def atender(self):
        while True:
            tid, m = self.cola.get()
            t = m["t"]
            if t == "LEER":
                p = m["p"]
                self.recuperar(p, invalidar=False)
                self.copias[p].add(tid)
                self.a(tid, {"t": "DATOS", "p": p, "modo": LECTURA,
                             "d": b64(self.datos[p])})
            elif t == "ESCRIBIR":
                p = m["p"]
                if self.dueno[p] != tid:
                    self.recuperar(p, invalidar=True)
                for c in list(self.copias[p]):
                    if c != tid:
                        self.esperar(c, {"t": "INVALIDA", "p": p},
                                     (c, "ACK", p))
                self.copias[p] = set()
                self.dueno[p] = tid
                self.a(tid, {"t": "DATOS", "p": p, "modo": ESCRITURA,
                             "d": b64(self.datos[p])})
            elif t == "PING":
                self.a(tid, {"t": "PONG", "p": -1})
            elif t == "BARRERA":
                self.barrera.append(tid)
                if len(self.barrera) == self.n_trab:
                    for b in self.barrera:
                        self.a(b, {"t": "SIGUE", "p": -1})
                    self.barrera = []
            elif t == "CONTADOR":
                self.a(tid, {"t": "CUENTA", "p": -1, "n": self.msgs})
            elif t == "RESULTADO":
                self.resultados[tid] = m["r"]
                if len(self.resultados) == self.n_trab:
                    self.listo.set()

    def correr(self, host, puerto):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((host, puerto))
        srv.listen()
        print(f"[maestro] escuchando en {host}:{puerto}, "
              f"esperando {self.n_trab} trabajador(es)…", flush=True)
        threading.Thread(target=self.atender, daemon=True).start()
        while len(self.conex) < self.n_trab:
            sock, dir_ = srv.accept()
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            hola = recibir(sock)
            tid = hola["id"]
            self.conex[tid] = (sock, threading.Lock())
            print(f"[maestro] trabajador {tid} conectado desde {dir_[0]} "
                  f"({hola.get('host', '?')})", flush=True)
            threading.Thread(target=self.lector, args=(tid, sock),
                             daemon=True).start()
        for tid in self.conex:
            self.a(tid, {"t": "SIGUE", "p": -1})
        self.listo.wait()
        imprimir_resumen(self.resultados)
        with open("resultados_dsm.json", "w", encoding="utf-8") as f:
            json.dump(self.resultados, f, indent=2, ensure_ascii=False)
        print("[maestro] resultados guardados en resultados_dsm.json")


# ---------------------------------------------------------------- trabajador
class Trabajador:
    def __init__(self, tid, host, puerto):
        self.tid = tid
        for intento in range(60):          # el maestro puede tardar en abrir
            try:
                self.sock = socket.create_connection((host, puerto), timeout=5)
                break
            except OSError:
                time.sleep(0.5)
        else:
            sys.exit(f"No se pudo conectar al maestro {host}:{puerto}")
        self.sock.settimeout(None)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.cand_env = threading.Lock()
        self.cand = threading.Lock()
        self.cache = {}          # p -> [modo, bytearray]
        self.resp = queue.Queue()
        self.fallos = 0
        self.invalidaciones = 0
        enviar(self.sock, {"t": "HOLA", "id": tid,
                           "host": socket.gethostname()}, self.cand_env)
        threading.Thread(target=self.lector, daemon=True).start()

    def lector(self):
        while True:
            m = recibir(self.sock)
            if m is None:
                os._exit(0)
            t, p = m["t"], m["p"]
            if t == "DATOS":
                with self.cand:   # se instala aquí para respetar el orden TCP
                    self.cache[p] = [m["modo"], unb64(m["d"])]
                self.resp.put(m)
            elif t == "INVALIDA":
                with self.cand:
                    self.cache.pop(p, None)
                    self.invalidaciones += 1
                enviar(self.sock, {"t": "ACK", "p": p}, self.cand_env)
            elif t == "RECALL":
                with self.cand:
                    modo_datos = self.cache.get(p)
                    datos = modo_datos[1] if modo_datos else bytearray(PAGINA)
                    if m["inv"]:
                        self.cache.pop(p, None)
                        self.invalidaciones += 1
                    elif modo_datos:
                        modo_datos[0] = LECTURA
                enviar(self.sock, {"t": "DEVUELVE", "p": p, "d": b64(datos)},
                       self.cand_env)
            else:
                self.resp.put(m)

    def pedir(self, msg):
        enviar(self.sock, msg, self.cand_env)
        return self.resp.get()

    # -- API de memoria compartida: el programa solo ve leer/escribir
    def leer(self, dir_):
        p, off = divmod(dir_, PAGINA)
        while True:
            with self.cand:
                e = self.cache.get(p)
                if e is not None:
                    return struct.unpack_from("<q", e[1], off)[0]
            self.fallos += 1                    # "falla de página"
            self.pedir({"t": "LEER", "p": p})

    def escribir(self, dir_, v):
        p, off = divmod(dir_, PAGINA)
        while True:
            with self.cand:
                e = self.cache.get(p)
                if e is not None and e[0] == ESCRITURA:
                    struct.pack_into("<q", e[1], off, v)
                    return
            self.fallos += 1
            self.pedir({"t": "ESCRIBIR", "p": p})

    def barrera(self):
        self.pedir({"t": "BARRERA", "p": -1})

    def msgs_maestro(self):
        return self.pedir({"t": "CONTADOR", "p": -1})["n"]


# ---------------------------------------------------------------- experimentos
def experimentos(w, n_trab, duracion):
    r = {"host": socket.gethostname()}
    w.resp.get()   # SIGUE inicial: todos conectados

    # 1) RTT de red con el maestro (mensaje vacío ida y vuelta)
    t = []
    for _ in range(50):
        t0 = time.perf_counter()
        w.pedir({"t": "PING", "p": -1})
        t.append(time.perf_counter() - t0)
    t.sort()
    r["rtt_ms"] = t[len(t) // 2] * 1e3

    # 2) lectura local (página ya en caché) vs. falla remota (página nueva)
    base = (10 + w.tid * 8) * PAGINA        # páginas propias de este trabajador
    w.leer(base)
    t0 = time.perf_counter()
    for _ in range(100000):
        w.leer(base)
    r["lectura_local_us"] = (time.perf_counter() - t0) / 100000 * 1e6
    t = []
    for k in range(1, 8):
        t0 = time.perf_counter()
        w.leer(base + k * PAGINA)
        t.append(time.perf_counter() - t0)
    t.sort()
    r["falla_remota_ms"] = t[len(t) // 2] * 1e3
    w.barrera()

    # 3) falso compartimiento: cada trabajador incrementa SU variable.
    #    misma página: direcciones 0, 8, 16…  | páginas distintas: 0, 4096, 8192…
    for caso, salto in (("misma_pagina", 8), ("paginas_distintas", PAGINA)):
        dir_ = 2 * PAGINA + (w.tid - 1) * salto
        w.escribir(dir_, 0)
        w.barrera()
        f0, i0 = w.fallos, w.invalidaciones
        m0 = w.msgs_maestro()
        w.barrera()
        hechos, t0 = 0, time.perf_counter()
        while time.perf_counter() - t0 < duracion:   # todos trabajan a la vez
            w.escribir(dir_, w.leer(dir_) + 1)
            hechos += 1
        dt = time.perf_counter() - t0
        w.barrera()
        final = w.leer(dir_)
        r[caso] = {"seg": dt, "incrementos": hechos,
                   "por_seg": hechos / dt, "fallos": w.fallos - f0,
                   "invalidaciones": w.invalidaciones - i0,
                   "correcto": final == hechos}
        w.barrera()
        r[caso]["msgs_maestro"] = w.msgs_maestro() - m0
        w.barrera()

    r["duracion_s"] = duracion
    enviar(w.sock, {"t": "RESULTADO", "p": -1, "r": r}, w.cand_env)
    time.sleep(0.5)


def imprimir_resumen(res):
    print("\n=========== RESULTADOS DSM ===========")
    for tid in sorted(res, key=int):
        r = res[tid]
        print(f"\nTrabajador {tid} ({r['host']})")
        print(f"  RTT con el maestro ............ {r['rtt_ms']:.3f} ms")
        print(f"  Lectura local (en caché) ...... {r['lectura_local_us']:.3f} µs")
        print(f"  Falla de página remota ........ {r['falla_remota_ms']:.3f} ms")
        for caso in ("misma_pagina", "paginas_distintas"):
            c = r[caso]
            print(f"  {caso:18s} {c['incrementos']} incrementos en "
                  f"{c['seg']:.2f} s ({c['por_seg']:.0f}/s), {c['fallos']} fallas, "
                  f"{c['invalidaciones']} invalidaciones, "
                  f"correcto={c['correcto']}")


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawTextHelpFormatter)
    sub = ap.add_subparsers(dest="rol", required=True)
    m = sub.add_parser("maestro")
    m.add_argument("--host", default="0.0.0.0")
    m.add_argument("--puerto", type=int, default=5000)
    m.add_argument("--trabajadores", type=int, default=2)
    t = sub.add_parser("trabajador")
    t.add_argument("--maestro", required=True)
    t.add_argument("--puerto", type=int, default=5000)
    t.add_argument("--id", type=int, required=True)
    t.add_argument("--seg", type=float, default=3.0)
    lo = sub.add_parser("local")
    lo.add_argument("--trabajadores", type=int, default=2)
    lo.add_argument("--puerto", type=int, default=5000)
    lo.add_argument("--seg", type=float, default=3.0)
    a = ap.parse_args()

    if a.rol == "maestro":
        Maestro(a.trabajadores).correr(a.host, a.puerto)
    elif a.rol == "trabajador":
        w = Trabajador(a.id, a.maestro, a.puerto)
        print(f"[trabajador {a.id}] conectado a {a.maestro}:{a.puerto}",
              flush=True)
        experimentos(w, None, a.seg)
        print(f"[trabajador {a.id}] terminado", flush=True)
    else:
        hijos = []
        for i in range(1, a.trabajadores + 1):
            hijos.append(subprocess.Popen(
                [sys.executable, os.path.abspath(__file__), "trabajador", "--maestro", "127.0.0.1",
                 "--puerto", str(a.puerto), "--id", str(i),
                 "--seg", str(a.seg)]))
        Maestro(a.trabajadores).correr("127.0.0.1", a.puerto)
        for h in hijos:
            h.wait()


if __name__ == "__main__":
    main()
