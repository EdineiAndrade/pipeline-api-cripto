import requests
from fastapi import FastAPI, HTTPException, BackgroundTasks
from sqlalchemy import create_engine, Column, String, Float, Integer, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import logging
import asyncio
import time

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Carrega variáveis de ambiente
load_dotenv()

# Configurações do banco de dados
DATABASE_URL = os.getenv('DB_URL')
if not DATABASE_URL:
    raise ValueError("Variável de ambiente DB_URL não configurada")

# Criação do engine
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Modelo de dados
class CryptoPrice(Base):
    __tablename__ = 'crypto_prices'
    id = Column(Integer, primary_key=True, autoincrement=True)
    valor = Column(Float)
    cripto = Column(String(10))
    moeda = Column(String(10))
    timestamp = Column(DateTime)

# Inicialização do banco
def init_db():
    Base.metadata.create_all(bind=engine)

# Gerenciador de sessão
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Funções de negócio
def extrair_dados():
    url = "https://api.coinbase.com/v2/prices/BTC-USD/spot"  # URL mais específica
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        logger.info(f"Dados recebidos: {response.json()}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao acessar API Coinbase: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao acessar API externa")

def tratar_dados_cripto(dados_json):
    try:
        valor = float(dados_json['data']['amount'])
        cripto = dados_json['data']['base']
        moeda = dados_json['data']['currency']
        
        return CryptoPrice(
            valor=valor,
            cripto=cripto,
            moeda=moeda,
            timestamp=datetime.utcnow()  # Usando UTC para consistência
        )
    except (KeyError, ValueError) as e:
        logger.error(f"Erro ao processar dados: {str(e)}")
        raise HTTPException(status_code=400, detail="Dados recebidos em formato inválido")

def salvar_dados_sqlalchemy(dados, db):
    try:
        db.add(dados)
        db.commit()
        logger.info(f"Dados inseridos: {dados.cripto} {dados.valor} {dados.moeda}")
    except Exception as e:
        db.rollback()
        logger.error(f"Erro ao salvar dados: {str(e)}")
        raise HTTPException(status_code=500, detail="Erro ao salvar no banco de dados")

# Tarefa de background para coleta periódica
async def coletar_dados_task():
    while True:
        try:
            with SessionLocal() as db:
                dados_json = extrair_dados()
                dados_tratados = tratar_dados_cripto(dados_json)
                salvar_dados_sqlalchemy(dados_tratados, db)
        except Exception as e:
            logger.error(f"Erro no coletor: {str(e)}")
        
        await asyncio.sleep(60)  # Espera 1 minuto

# Configuração do FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    # Inicia a tarefa de background apenas na Vercel
    if os.getenv('VERCEL') == '1':
        asyncio.create_task(coletar_dados_task())
    yield
    engine.dispose()

app = FastAPI(lifespan=lifespan)

# Rotas da API
@app.get("/")
async def root():
    return {"message": "API de Monitoramento de Criptomoedas"}

@app.get("/healthcheck")
async def healthcheck():
    return {"status": "healthy"}

@app.post("/trigger-collect")
async def trigger_collect(background_tasks: BackgroundTasks):
    background_tasks.add_task(
        lambda: asyncio.create_task(coletar_dados_task())
    )
    return {"message": "Coleta iniciada em background"}

# Função mantida para compatibilidade com execução local
def main():
    init_db()
    while True:
        try:
            with SessionLocal() as db:
                dados_json = extrair_dados()
                dados_tratados = tratar_dados_cripto(dados_json)
                salvar_dados_sqlalchemy(dados_tratados, db)
        except Exception as e:
            logger.error(f"Erro no pipeline: {str(e)}")
        
        time.sleep(60)

if __name__ == "__main__":
    main()