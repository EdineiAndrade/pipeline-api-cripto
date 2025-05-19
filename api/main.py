import requests
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime, func
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import logging
import asyncio
import traceback

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Carrega variáveis de ambiente
load_dotenv()

# Configurações do banco de dados
DATABASE_URL = os.getenv('DB_URL')
if not DATABASE_URL:
    logger.error("Variável de ambiente DB_URL não configurada")
    raise ValueError("DB_URL não configurada")

# Conexão otimizada para a Vercel
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    connect_args={
        "connect_timeout": 5,
        "keepalives": 1,
        "keepalives_idle": 30,
        "options": "-c timezone=utc"
    }
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Modelo de dados
class CryptoPrice(Base):    
    __tablename__ = 'crypto_prices'
    id = Column(Integer, primary_key=True, autoincrement=True)
    valor = Column(Float, nullable=False)
    cripto = Column(String(10), nullable=False)
    moeda = Column(String(10), nullable=False)
    timestamp = Column(DateTime(timezone=False), nullable=False)

    def __repr__(self):
        return f"<CryptoPrice {self.cripto}={self.valor} {self.moeda} {self.timestamp}>"

# Inicialização do banco
def init_db():
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Tabelas criadas com sucesso")
    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {str(e)}")
        raise

# Funções de negócio
def extrair_dados():
    url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Dados recebidos da API: {data}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro na API Coinbase: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao acessar API: {str(e)}"
        )

def tratar_dados_cripto(dados_json):
    try:
        dt = datetime.now()    
        dt_naive = dt.replace(tzinfo=None)
        return CryptoPrice(
            valor=float(dados_json['data']['amount']),
            cripto=dados_json['data']['base'],
            moeda=dados_json['data']['currency'],
            timestamp=dt_naive
        )
    except (KeyError, ValueError) as e:
        logger.error(f"Erro ao processar dados: {str(e)} - Dados: {dados_json}")
        raise HTTPException(
            status_code=400,
            detail=f"Dados inválidos: {str(e)}"
        )

def salvar_dados_sqlalchemy(dados, db):
    try:
        db.add(dados)
        db.commit()
        db.refresh(dados)
        logger.info(f"Dados salvos: {dados}")
        return dados
    except Exception as e:
        db.rollback()
        logger.error(f"Erro ao salvar: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro no banco de dados: {str(e)}"
        )

# Configuração do FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if os.getenv('VERCEL') == '1':
        # Na Vercel, iniciamos a coleta automática
        async def coletar_periodicamente():
            while True:
                try:
                    with SessionLocal() as db:
                        dados = extrair_dados()
                        tratados = tratar_dados_cripto(dados)
                        salvar_dados_sqlalchemy(tratados, db)
                except Exception as e:
                    logger.error(f"Erro no coletor automático: {str(e)}")
                await asyncio.sleep(60)  # Espera 1 minuto
        
        asyncio.create_task(coletar_periodicamente())
    yield
    await engine.dispose()

app = FastAPI(
    title="API de Criptomoedas",
    lifespan=lifespan
)

# Rotas da API
@app.get("/")
async def root():
    return {"message": "API de Monitoramento de Criptomoedas"}

@app.post("/salvar")
async def salvar():
    """Endpoint para acionar manualmente a coleta e salvamento"""
    with SessionLocal() as db:
        try:
            dados = extrair_dados()
            tratados = tratar_dados_cripto(dados)
            resultado = salvar_dados_sqlalchemy(tratados, db)
            return {
                "status": "success",
                "id": resultado.id,
                "dados": {
                    "cripto": resultado.cripto,
                    "valor": resultado.valor,
                    "moeda": resultado.moeda
                }
            }
        except Exception as e:
            logger.error(traceback.format_exc())
            raise

@app.get("/ultimos-registros")
async def ultimos_registros(limit: int = 5):
    """Endpoint para verificar os últimos registros salvos"""
    with SessionLocal() as db:
        registros = db.query(CryptoPrice)\
                    .order_by(CryptoPrice.timestamp.desc())\
                    .limit(limit)\
                    .all()
        return {
            "total": len(registros),
            "dados": [
                {
                    "id": r.id,
                    "cripto": r.cripto,
                    "valor": r.valor,
                    "moeda": r.moeda,
                    "timestamp": r.timestamp.isoformat()
                } for r in registros
            ]
        }

# Função para execução local
def main():
    """Função para execução local do coletor contínuo"""
    init_db()
    while True:
        try:
            with SessionLocal() as db:
                dados = extrair_dados()
                tratados = tratar_dados_cripto(dados)
                salvar_dados_sqlalchemy(tratados, db)
        except Exception as e:
            logger.error(f"Erro no coletor local: {str(e)}")
        
        import time
        time.sleep(60)  # Espera 1 minuto

if __name__ == "__main__":
    main()