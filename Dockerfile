# Dockerfile
# Etapa 1: Imagem base
FROM python:3.12-slim

# Etapa 2: Configuração do ambiente
# Atualiza o sistema e instala dependências essenciais
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Define a variável de ambiente para evitar criação de .pyc
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Etapa 3: Criação de diretórios
WORKDIR /app

# Copia os arquivos do projeto para o container
COPY . /app

# Etapa 4: Instalação de dependências
# Instala o uv para gerenciar pacotes (caso esteja usando)
RUN pip install uv

# Instala as dependências listadas no arquivo pyproject.toml
RUN uv sync

# Etapa 5: Configuração de entrada padrão
# Define o comando padrão para o container (caso precise usar um script inicial)
CMD ["python", "scripts/extract.py"]
