# Use a imagem base do Python
FROM python:3.13-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia os arquivos de dependência para o contêiner
COPY requirements.txt app/

RUN pip install -r app/requirements.txt

# Copia o restante do código para o contêiner
COPY . /app/

EXPOSE 8081

# Mantem o container em execução
CMD tail -f /dev/null
