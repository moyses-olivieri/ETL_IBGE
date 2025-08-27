#imagem
FROM python:3.13-slim

#diretório
WORKDIR /app

#requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#Copia o código fonte
COPY src/ ./src/

#Comando para rodar o container docker
CMD ["python", "-m", "src.main"]
