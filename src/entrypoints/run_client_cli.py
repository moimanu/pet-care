from __future__ import annotations

import argparse
from typing import Any, Callable
import Pyro5.api


class ClientCLI:
    """Interactive terminal dashboard for tutor communicating exclusively with Middleware."""

    def __init__(self, middleware_proxy: Any) -> None:
        self.middleware = middleware_proxy

    def display_menu(self) -> None:
        print("\n==================================================")
        print("           SISTEMA IOT PET CARE - DASHBOARD       ")
        print("==================================================")
        print(" [1] Visualizar Status Geral do Pet")
        print(" [2] Dispensar Ração Manualmente")
        print(" [3] Abastecer Água Manualmente")
        print(" [4] Resetar Caixa de Areia (Registrar Limpeza)")
        print(" [5] Silenciar / Desarmar Alarme de Fuga")
        print(" [0] Sair")
        print("==================================================")

    def view_status(self) -> None:
        try:
            res = self.middleware.get_pet_status()
            if isinstance(res, dict) and not res.get("sucesso", True):
                print(f"[ERRO] {res.get('mensagem', 'Falha ao consultar estado')}")
                return

            status = res.get("estado") if (isinstance(res, dict) and res.get("estado") is not None) else res
            print("\n----- STATUS GERAL DO PET (QUÓRUM BIZANTINO) -----")
            print(f" • Ração Disponível : {status.get('racao_gramas', 0.0):.1f} g")
            print(f" • Água Disponível  : {status.get('agua_ml', 0.0):.1f} ml")
            print(f" • Usos da Areia    : {status.get('areia_usos', 0)}")
            print(f" • Perímetro / Fuga : {status.get('status_fuga', 'DESCONHECIDO')}")
            print(f" • Alarme Ativo     : {'SIM (DISPARADO!)' if status.get('alarme_ativo') else 'NÃO'}")
            print(f" • Última Leitura   : {status.get('atualizado_em', 'N/A')}")
            print(f" • Hash de Consenso : {status.get('hash', 'N/A')[:16]}...")
            print("--------------------------------------------------")
        except Exception as exc:
            print(f"[ERRO] Falha ao consultar estado consolidado: {exc}")

    def _invoke(self, action_fn: Callable[[], dict[str, Any]], error_msg: str) -> None:
        try:
            res = action_fn()
            print(f"[{'SUCESSO' if res.get('sucesso') else 'FALHA'}] {res.get('mensagem')}")
        except Exception as exc:
            print(f"[ERRO] {error_msg}: {exc}")

    def dispense_food(self, qty_str: str = "50") -> None:
        qty = float(qty_str) if qty_str.strip() else 50.0
        self._invoke(lambda: self.middleware.dispensar_racao(qty), "Falha ao dispensar ração")

    def refill_water(self, qty_str: str = "200") -> None:
        qty = float(qty_str) if qty_str.strip() else 200.0
        self._invoke(lambda: self.middleware.repor_agua(qty), "Falha ao repor água")

    def reset_litter(self) -> None:
        self._invoke(self.middleware.resetar_areia, "Falha ao resetar areia")

    def silence_alarm(self) -> None:
        self._invoke(self.middleware.silenciar_alarme, "Falha ao silenciar alarme")

    def handle_option(self, option: str) -> bool:
        opt = option.strip()
        if opt == "1":
            self.view_status()
        elif opt == "2":
            qty = input("Quantidade de ração em gramas [50]: ")
            self.dispense_food(qty)
        elif opt == "3":
            qty = input("Quantidade de água em ml [200]: ")
            self.refill_water(qty)
        elif opt == "4":
            self.reset_litter()
        elif opt == "5":
            self.silence_alarm()
        elif opt == "0":
            print("Encerrando dashboard.")
            return False
        else:
            print("Opção inválida.")
        return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Cliente CLI Pet Care")
    parser.add_argument("--middleware", default="PYRO:petcare.middleware@localhost:9090", help="URI Pyro5 do Middleware")
    args = parser.parse_args()

    proxy = Pyro5.api.Proxy(args.middleware)
    cli = ClientCLI(middleware_proxy=proxy)

    while True:
        cli.display_menu()
        try:
            opt = input("Selecione uma opção: ")
            if not cli.handle_option(opt):
                break
        except (KeyboardInterrupt, EOFError):
            print("\nEncerrando dashboard.")
            break


if __name__ == "__main__":
    main()
