from collections import Counter
import uuid
import Pyro5.api

NOS = (1, 2, 3)
QUORUM = 2


def conectar_no(id_no, timeout=1.0):
    conexao = Pyro5.api.Proxy(f"PYRO:controlador.{id_no}@localhost:{9090 + id_no}")
    conexao._pyroTimeout = timeout
    return conexao


def consultar_quorum():
    respostas = []
    for id_no in NOS:
        try:
            respostas.append(conectar_no(id_no).consultar_estado())
        except Exception:
            pass

    if len(respostas) < QUORUM:
        return None, f"Sem quorum ({len(respostas)}/3 nos online)"

    racao, votos = Counter(resposta["racao"] for resposta in respostas).most_common(1)[0]
    if votos < QUORUM:
        return None, f"Divergencia entre nos ({votos}/3 concordam)"

    nos_vivos = {resposta["id_no"] for resposta in respostas}
    lider = next((resposta["lider_id"] for resposta in respostas if resposta["lider_id"] in nos_vivos), None)

    if lider is None:
        candidato = min(nos_vivos)
        try:
            if conectar_no(candidato, 1.5).eleger_lider():
                return consultar_quorum()
        except Exception:
            pass

    return {"racao": racao, "lider": lider, "votos": votos, "vivos": sorted(nos_vivos)}, None


def enviar_liberacao_racao():
    id_comando = str(uuid.uuid4())
    for _ in NOS:
        estado, erro = consultar_quorum()
        if erro:
            return print(f"[ERRO] {erro}")
        lider = estado["lider"]
        if lider is None:
            return print("[ERRO] Nenhum lider eleito")
        try:
            resposta = conectar_no(lider, 2.0).liberar_racao(id_comando)
            if resposta.get("ok"):
                estado_pos_escrita, erro_pos_escrita = consultar_quorum()
                if (
                    not erro_pos_escrita
                    and lider in estado_pos_escrita["vivos"]
                    and estado_pos_escrita["racao"] == resposta["racao"]
                ):
                    tag = " (idempotente)" if resposta.get("idempotente") else ""
                    return print(
                        f"[OK] Racao liberada pelo Lider {lider}{tag}! Nivel atual: {resposta['racao']}g"
                    )
                print(f"[FAILOVER] Resposta do Lider {lider} diverge do quorum, tentando outro...")
                continue
            print(f"[FAILOVER] No {lider} recusou ({resposta.get('erro')}), tentando outro...")
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
