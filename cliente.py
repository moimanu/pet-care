import uuid
import Pyro5.api

NOS = (1, 2, 3)
QUORUM = 2


def conectar_no(id_no, timeout=1.0):
    conexao = Pyro5.api.Proxy(f"PYRO:controlador.{id_no}@localhost:{9090 + id_no}")
    conexao._pyroTimeout = timeout
    return conexao


def tentar_eleger_lider(candidatos):
    for id_no in candidatos:
        try:
            if conectar_no(id_no, 1.5).eleger_lider():
                return id_no
        except Exception:
            pass
    return None


def consultar_quorum():
    respostas = []
    for id_no in NOS:
        try:
            respostas.append(conectar_no(id_no).consultar_estado())
        except Exception:
            pass

    if len(respostas) < QUORUM:
        return None, f"Sem quorum ({len(respostas)}/3 nos online)"

    racao = None
    hash_quorum = None
    votos = 0
    for referencia in respostas:
        contagem = 0
        for outra in respostas:
            if outra["racao"] == referencia["racao"] and outra["hash_estado"] == referencia["hash_estado"]:
                contagem += 1
        if contagem > votos:
            votos = contagem
            racao = referencia["racao"]
            hash_quorum = referencia["hash_estado"]

    if votos < QUORUM:
        return None, f"Divergencia de estado/hash entre nos ({votos}/3 concordam)"

    consistentes = []
    nos_vivos = []
    for resposta in respostas:
        if resposta["racao"] == racao and resposta["hash_estado"] == hash_quorum:
            consistentes.append(resposta)
            nos_vivos.append(resposta["id_no"])
    nos_vivos.sort()

    lider = None
    maior_mandato = -1
    for resposta in consistentes:
        if resposta["lider_id"] == resposta["id_no"] and resposta["mandato"] > maior_mandato:
            maior_mandato = resposta["mandato"]
            lider = resposta["id_no"]

    if lider is None:
        lider = tentar_eleger_lider(nos_vivos)

    return {"racao": racao, "lider": lider, "votos": votos, "vivos": nos_vivos}, None


def enviar_liberacao_racao():
    id_comando = str(uuid.uuid4())
    for _ in NOS:
        estado, erro = consultar_quorum()
        if erro:
            print(f"[ERRO] {erro}")
            return
        lider = estado["lider"]
        if lider is None:
            print("[ERRO] Nenhum lider eleito")
            return
        try:
            resposta = conectar_no(lider, 2.0).liberar_racao(id_comando)
            if resposta["ok"]:
                estado_confirmado, erro_confirmacao = consultar_quorum()
                if (
                    not erro_confirmacao
                    and lider in estado_confirmado["vivos"]
                    and estado_confirmado["racao"] == resposta["racao"]
                ):
                    if resposta["idempotente"]:
                        print(f"[OK] Racao liberada pelo Lider {lider} (idempotente)! Nivel atual: {resposta['racao']}g")
                    else:
                        print(f"[OK] Racao liberada pelo Lider {lider}! Nivel atual: {resposta['racao']}g")
                    return
                print(f"[FAILOVER] Resposta do Lider {lider} diverge do quorum, tentando outro...")
                continue

            print(f"[FAILOVER] No {lider} recusou ({resposta['erro']}), tentando outro...")
            outros_vivos = []
            for id_no in estado["vivos"]:
                if id_no != lider:
                    outros_vivos.append(id_no)
            tentar_eleger_lider(outros_vivos)
        except Exception as falha:
            print(f"[FAILOVER] Lider {lider} caiu ({falha}), elegendo novo lider...")
    print("[ERRO] Cluster indisponivel")


if __name__ == "__main__":
    while True:
        opcao = input("\n[1] Estado (quorum)  [2] Liberar racao  [0] Sair > ").strip()
        if opcao == "1":
            estado, erro = consultar_quorum()
            if erro:
                print(f"[ERRO] {erro}")
            else:
                print(
                    f"[QUORUM OK] Racao: {estado['racao']}g | Lider: {estado['lider']} | Nos: {estado['vivos']} ({estado['votos']}/3)"
                )
        elif opcao == "2":
            enviar_liberacao_racao()
        elif opcao == "0":
            break
