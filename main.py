from api.main import app, main
import os
import logging

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    if os.getenv('VERCEL') == '1':
        # Modo Vercel: roda apenas o FastAPI (o coletor automático é iniciado via lifespan em api/main.py)
        import uvicorn
        logger.info("Iniciando servidor FastAPI (modo Vercel)")
        uvicorn.run(app, host="0.0.0.0", port=8000)
    else:
        # Modo local: roda o coletor contínuo (main())
        logger.info("Iniciando coletor de dados (modo local)")
        main()