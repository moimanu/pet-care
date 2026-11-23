# app/domain.py

Módulo de domínio puro que define o modelo de estado do pet (`PetState`), o cálculo de hash canônico para verificação de consenso e as regras de validação e transição de estado para telemetria e comandos.

```python
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
import hashlib
import json

SENSOR_LIMITS = {
    "racao": ("nivel_gramas", 0.0, 500.0),
    "agua": ("nivel_ml", 0.0, 1000.0),
}


def utc_now_iso() -> str:
    # Gera um timestamp UTC atual formatado em ISO-8601 (YYYY-MM-DDTHH:MM:SSZ)
    # para registrar quando o estado ou evento foi atualizado.
    pass


@dataclass
class PetState:
    racao_gramas: float = 250.0
    agua_ml: float = 500.0
    areia_usos: int = 0
    status_fuga: str = "DENTRO"
    alarme_ativo: bool = False
    atualizado_em: str = ""

    def __post_init__(self) -> None:
        # Garante que toda instância criada sem timestamp explícito receba automaticamente
        # o horário UTC atual como valor inicial de `atualizado_em`.
        pass

    def to_dict(self) -> dict:
        # Serializa os atributos do dataclass `PetState` para um dicionário Python padrão,
        # facilitando o envio via Pyro5, JSON e persistência em SQLite.
        pass

    @classmethod
    def from_dict(cls, data: dict) -> "PetState":
        # Reconstrói uma instância tipada de `PetState` a partir de um dicionário bruto,
        # convertendo explicitamente cada campo para seu tipo esperado e preenchendo o timestamp se ausente.
        pass

    def canonical_hash(self) -> str:
        # Calcula um resumo SHA-256 determinístico (12 caracteres) sobre os campos de negócio do estado,
        # arredondando valores decimais e excluindo `atualizado_em` para permitir comparação justa entre réplicas.
        pass


def validate_telemetry(sensor: str, payload: dict) -> dict:
    # 1. Valida se o payload recebido é um dicionário JSON válido.
    # 2. Para sensores numéricos contínuos ("racao" e "agua"), verifica tipo (rejeitando booleanos)
    #    e garante que o valor esteja dentro da faixa física definida em `SENSOR_LIMITS`.
    # 3. Para "areia", exige inteiro não-negativo em `usos_acumulados`; para "fuga", exige "DENTRO" ou "FORA" em `perimetro`.
    # 4. Lança `ValueError` caso o sensor seja desconhecido ou os dados violem o contrato, retornando o payload higienizado caso contrário.
    pass


def _actuator(topic: str, **payload) -> dict:
    # Função auxiliar que padroniza a estrutura de um comando de atuador MQTT,
    # encapsulando o tópico completo (`petcare/atuadores/<topic>`) e o dicionário de payload.
    pass


def apply_telemetry(state: PetState, sensor: str, payload: dict) -> tuple[PetState, list[dict]]:
    # 1. Valida os dados de telemetria e cria uma nova cópia imutável do estado com timestamp atualizado.
    # 2. Atualiza o campo correspondente ao sensor ("racao", "agua", "areia" ou "fuga").
    # 3. Caso o sensor de fuga indique "FORA", ativa o alarme no estado e gera um comando de atuador
    #    para disparar o alarme remotamente.
    # 4. Retorna a tupla contendo o novo `PetState` e a lista de comandos de atuadores a publicar.
    pass


def apply_command(state: PetState, action: str) -> tuple[PetState, list[dict]]:
    # 1. Cria uma nova cópia do estado atual com timestamp renovado para aplicar a ação solicitada.
    # 2. Interpreta comandos suportados ("LIBERAR_RACAO", "REPOR_AGUA", "RESETAR_AREIA", "SILENCIAR_ALARME"),
    #    aplicando tetos máximos de capacidade e gerando os respectivos comandos de atuadores MQTT quando aplicável.
    # 3. Lança `ValueError` para comandos desconhecidos e retorna o novo estado junto aos atuadores gerados.
    pass
```

