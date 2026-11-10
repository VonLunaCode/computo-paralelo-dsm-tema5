# Memoria compartida distribuida y modelos de consistencia

**Cómputo Paralelo y Distribuido · Tema 5 · Equipo 5**
Exposición: 10 de noviembre de 2026

| Integrante | Sección |
|---|---|
| Ever Garcia Ramirez | 1. Qué es la DSM y cómo se simula |
| Christian Luna Ortiz | 2. Esquemas: páginas, variables y objetos |
| Marcelino Parra Hernández | 3. Modelos de consistencia |
| José José Varela Palacios | 4. Acceso del Equipo A a datos del Equipo B |
| Christian Luna Ortiz | 5. Prueba real en un clúster de laptops |

## Contenido del repositorio

```
presentacion/
  presentacion.tex       código fuente de la presentación (LaTeX Beamer)
  presentacion.pdf       presentación compilada
prueba/
  dsm_demo.py            prototipo de DSM usado para las mediciones
  resultados/            mediciones reales que aparecen en la presentación
    loopback_laptop.json     maestro y trabajadores en la misma máquina
    internet_tailscale.json  trabajadores en una laptop, maestro en un servidor remoto (VPN)
```

## La prueba: un prototipo de DSM

`prueba/dsm_demo.py` es una **memoria compartida distribuida basada en páginas**, escrita en Python usando solo la biblioteca estándar (Python 3.8 o superior, en Linux, Windows o macOS):

- El espacio compartido son 128 páginas de **4 KB** (512 KiB).
- Hay un **gestor central** (el maestro) que guarda el directorio: quién es dueño de cada página y quién tiene copia.
- Los **trabajadores** se conectan al maestro por **sockets TCP**. Los mensajes son JSON con prefijo de longitud: `LEER`, `ESCRIBIR`, `INVALIDA`, `ACK` y `RECALL`.
- El protocolo es de **escritura-invalidación** con un solo escritor por página, al estilo de IVY. Para escribir, un nodo pide la propiedad exclusiva. El gestor invalida todas las demás copias y espera la confirmación (ACK) de cada nodo antes de concederla. Esto da consistencia secuencial.

Cada trabajador mide cuatro cosas:

1. **RTT** con el maestro: mediana de 50 pings.
2. **Lectura local** de una página que ya está en caché: promedio de 100 000 lecturas.
3. **Falla de página remota**: mediana de 7 páginas nuevas.
4. **Falso compartimiento**. Dos trabajadores incrementan, al mismo tiempo, *cada uno su propia variable*, durante un tiempo fijo:
   - `paginas_distintas`: cada variable está en su propia página.
   - `misma_pagina`: las dos variables están en la misma página, separadas por 8 bytes.

   Al final se verifica que ningún incremento se haya perdido (`correcto=True`).

### Cómo correrlo

Todo en una sola máquina:

```bash
cd prueba
python3 dsm_demo.py local --trabajadores 2 --seg 3
```

En un clúster de laptops en la misma red local:

```bash
# laptop maestra (anota su IP, p. ej. 192.168.1.10)
python3 dsm_demo.py maestro --trabajadores 2 --puerto 5000

# en cada laptop trabajadora
python3 dsm_demo.py trabajador --maestro 192.168.1.10 --puerto 5000 --id 1 --seg 3
python3 dsm_demo.py trabajador --maestro 192.168.1.10 --puerto 5000 --id 2 --seg 3
```

El maestro guarda todas las mediciones en `resultados_dsm.json`.

## Resultados medidos

Latencia de una falla de página:

| Red | RTT | Falla remota | Penalización vs. lectura local (≈0.30 µs) |
|---|---|---|---|
| Misma máquina (loopback) | 0.06–0.10 ms | 0.21 ms | ≈ 700× |
| Internet (laptop ↔ servidor, VPN) | 111 ms | 113 ms | ≈ 376 000× |

Falso compartimiento (incrementos por segundo, suma de los 2 trabajadores):

| | Loopback | Internet |
|---|---|---|
| Páginas distintas | 2.53 M/s (0 fallas) | 2.52 M/s (0 fallas) |
| Misma página | 1.14 M/s | 33 k/s |
| **Cuánto más lento** | **2.2×** | **76×** |

En todas las corridas el resultado final fue correcto: el protocolo mantiene la coherencia, y el falso compartimiento solo cuesta tiempo.

**Notas.** Las IPs del diagrama de la presentación son ilustrativas. La falla de página se simula en software: `leer`/`escribir` revisan la caché local. Un sistema real como IVY usa la MMU (por ejemplo, `mprotect` en Linux), pero el protocolo de red es el mismo.

## Compilar la presentación

Necesita TeX Live completo (usa `metropolis`, `fontawesome5`, `FiraSans` y `pgfplots`):

```bash
cd presentacion
latexmk -pdf presentacion.tex
```

O con Docker, sin instalar nada:

```bash
docker run --rm -v "$PWD/presentacion":/w -w /w texlive/texlive:latest latexmk -pdf presentacion.tex
```

## Referencias

- Tanenbaum, A. y Van Steen, M. *Sistemas distribuidos: principios y paradigmas*.
- Li, K. y Hudak, P. (1989). Memory coherence in shared virtual memory systems. *ACM TOCS*.
- Lamport, L. (1979). How to make a multiprocessor computer that correctly executes multiprocess programs. *IEEE Trans. Computers*.
- Bennett, J. et al. (1990). Munin: distributed shared memory based on type-specific memory coherence. *PPOPP*.
- Keleher, P. et al. (1994). TreadMarks: distributed shared memory on standard workstations and operating systems. *USENIX*.
