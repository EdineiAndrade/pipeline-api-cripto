from api.main import app, main  # Importa tanto o app FastAPI quanto a função main
import os

if __name__ == "__main__":
    if os.getenv('VERCEL') != '1':  # Executa apenas localmente
        # Roda o coletor de dados (loop infinito)
        main()
    else:
        # Roda apenas o servidor FastAPI na Vercel
        import uvicorn
        uvicorn.run(app, host="0.0.0.0", port=8000)