# Desafio Engenharia de Dados

## :nerd_face: Descrição do projeto

> O objetivo do projeto é realizar uma POC (proof of concept) para o desenvolvimento de um novo datalake para a empresa SiCooperative LTDA. A primeira estrutura a ser considerada e que foi utilizada para este projeto é a de movimentação de cartões.

## 📌 Índice
- [Sobre o Projeto](#-sobre-o-projeto)
- [Arquitetura de Dados](#-arquitetura-de-dados)
- [Melhorias Futuras](#-melhorias-futuras)
- [Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [Como Rodar o Projeto](#-como-rodar-o-projeto)
- [Imagens e Diagramas](#-imagens-e-diagramas)

---

## 📖 Sobre o Projeto

📌 A SiCooperative LTDA. enfrenta um problema de velocidade e assertividade na tomada de decisões causada pela ineficiência na disponibilização de informações, hoje muito tempo é perdido na criação de relatórios individuais e na tentativa de correlacioná-los manualmente.

> Este projeto tem como objetivo ser o primeiro passo para o desenvolvimento de um datalake que possibilitará a centralização de informações estratégicas.

> A estrutura de cartões segue a seguinte estrutura:

![tabela_silver](img/tabelas_silver.png)

> O objetivo é modelar esta estrutura em um banco de dados e ao final exportar um arquivo .csv com a seguinte estrutura:

![tabela_gold](img/tabela_gold.png)


Foram utilizados arquivos ficticios de diversos formatos como fonte de dados para a ingestão no banco de dados, a ideia foi simular as diversas fontes de dados que existem em um cenário real:

- associado: associado.csv
- cartão: cartao.json
- conta: conta.xml
- movimento do cartão: movimento.parquet

Foi utilizado a arquitetura medallion em camadas bronze, silver e gold para divisão lógica de camadas.

---

## :building_construction: Arquitetura de Dados

> A arquitetura de dados proposta para esta primeira estrutura de cartões é a seguinte:

![arquitetura](img/arquitetura.png)

- PostgreSQL: A escolha se deu pela sua confiabilidade e escalabilidade.
- Spark: Framework de processamento distribuido para big data de alta performance e escalabilidade.
- Python: Linguagem de fácil manutenção e que se conecta facilmente com o Spark, PostgreSQL e outras ferramentas e framework de dados.

## :rocket: Melhorias Futuras

Para uma melhoria futura da arquitetura a recomendação é a adição das seguintes ferramentas:

- Orquestração: Apache Airflow para controlar e automatizar a pipeline de dados e execução de scripts.
- Processamento Real-Time: Apache Kafka para casos como prevençao à fraudes um framework real-time é importante.
- Segurança e controle de acesso: Implementar um controle por roles no PostgreSQL por usuário.
- Cloud: Pode-se migrar a arquitetura para a nuvem AWS dado sua escalabilidade e menor custo.

## 🛠 Tecnologias Utilizadas

As principais tecnologias usadas no projeto são:

- **🛠 Linguagem:** Python 3.9
- **📊 Data Pipeline:** Apache Spark
- **🛢 Banco de Dados:** PostgreSQL
- **🐳 Containers:** Docker e Docker Compose

---

## 🚀 Como Rodar o Projeto

### **1️⃣ Pré-requisitos**
Antes de iniciar, instale:
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

#### Para rodar o container siga o seguinte passo a passo:

> Abrir o terminal e digitar:

- cd caminho_do_diretorio
- docker-compose build --no-cache
- docker-compose up -d

### **2️⃣ Clone o repositório**
```sh
git clone https://github.com/seu-usuario/seu-projeto.git
cd seu-projeto


![Diagrama do Projeto](img/arquitetura.png)
