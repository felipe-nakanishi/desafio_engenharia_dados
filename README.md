# Desafio Engenharia de Dados

## :nerd_face: Descrição do projeto

> O objetivo do projeto é realizar uma POC (proof of concept) para o desenvolvimento de um novo datalake para a empresa SiCooperative LTDA. A primeira estrutura a ser considerada e que foi utilizada para este projeto é a de movimentação de cartões.

## 📌 Índice
- [Sobre o Projeto](#-sobre-o-projeto)
- [Tecnologias Utilizadas](#-tecnologias-utilizadas)
- [Como Rodar o Projeto](#-como-rodar-o-projeto)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Imagens e Diagramas](#-imagens-e-diagramas)
- [Contribuição](#-contribuição)
- [Licença](#-licença)

---

## 📖 Sobre o Projeto

📌 A SiCooperative LTDA. enfrenta um problema de velocidade e assertividade na tomada de decisões causada pela ineficiência na disponibilização de informações, hoje muito tempo é perdido na criação de relatórios individuais e na tentativa de correlacioná-los manualmente.

> Este projeto tem como objetivo ser o primeiro passo para o desenvolvimento de um datalake que possibilitará a centralização de informações estratégicas.

> A estrutura de cartões segue a seguinte estrutura:

![tabela_silver](img/tabelas_silver.png)

> O objetivo é modelar esta estrutura em um banco de dados e ao final exportar um arquivo .csv com a seguinte estrutura:

![tabela_gold](img/tabela_gold.png)


Foram utilizados arquivos ficticios de diversos formatos como fonte de dados para a ingestão no banco de dados, a ideia foi simular as diversas fontes de dados que existem em um cenário real:

*** associado: *** associado.csv
*** cartão: cartao.json
*** conta: conta.xml
*** movimento do cartão: movimento.parquet

---

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

### **2️⃣ Clone o repositório**
```sh
git clone https://github.com/seu-usuario/seu-projeto.git
cd seu-projeto


![Diagrama do Projeto](img/arquitetura.png)
