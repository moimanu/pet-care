# Presets de teste no terminal:
# 1) Adulterar 1 no (quorum 2/3 continua valido):  .venv/bin/python adulterar.py 3 123
# 2) Adulterar lider com valor fora de 0..500g:   .venv/bin/python adulterar.py 1 999
# 3) Adulterar 2 nos diferentes (quebra quorum):  .venv/bin/python adulterar.py 1 111 && .venv/bin/python adulterar.py 2 222
# 4) Divergencia total nos 3 nos (111, 222, 333): .venv/bin/python adulterar.py --divergencia-total

import os
import sqlite3
import sys


def adulterar_no(id_no, nova_racao):
    caminho_db = f"no_{id_no}.db"
    if not os.path.exists(caminho_db):
        print(f"[ERRO] Banco {caminho_db} nao encontrado. Inicie os controladores primeiro.")
        return False

    with sqlite3.connect(caminho_db, timeout=5.0) as banco:
        linha = banco.execute("SELECT racao FROM estado WHERE id=1").fetchone()
        anterior = linha[0] if linha else None
        banco.execute("UPDATE estado SET racao=? WHERE id=1", (nova_racao,))

    print(f"[ADULTERAR] {caminho_db}: racao {anterior}g -> {nova_racao}g")
    return True


if __name__ == "__main__":
    if sys.argv[1:] == ["--divergencia-total"]:
        for id_no, valor in ((1, 111), (2, 222), (3, 333)):
            adulterar_no(id_no, valor)
    elif len(sys.argv) == 3 and sys.argv[1] in ("1", "2", "3"):
        if not adulterar_no(int(sys.argv[1]), int(sys.argv[2])):
            sys.exit(1)
    else:
        print("Uso: python adulterar.py <1|2|3> <racao> | --divergencia-total")
        sys.exit(1)
