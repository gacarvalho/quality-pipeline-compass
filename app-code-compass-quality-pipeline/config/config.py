from pydantic import BaseSettings
from datetime import datetime

class Config(BaseSettings):
    """
    Classe de configuração da aplicação.
    Utiliza Pydantic para gerenciar e validar as configurações.
    """

    odate = datetime.now().strftime("%Y%m%d")

    # Caminho base para os dados
    base_path: str = "/santander"

    # Caminhos para dados rejeitados
    path_rejeitados_schema: str = f"/santander/quality/compass/reviews/schema/odate={odate}"
    path_rejeitados_pattern_google: str = f"/santander/quality/compass/reviews/pattern/google_play/odate={odate}"
    path_rejeitados_pattern_apple: str = f"/santander/quality/compass/reviews/pattern/apple_store/odate={odate}"
    path_rejeitados_pattern_mongo: str = f"/santander/quality/compass/reviews/pattern/internal_databases/odate={odate}"

    class Config:
        env_file = ".env"
