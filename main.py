import requests
import re
import logging
import os
import functions as fun
from datetime import datetime

# === CONFIGURAÇÃO DO LOG ===

# Diretório onde os logs serão salvos
log_dir = "logs_pedidos_de_compras"
os.makedirs(log_dir, exist_ok=True)

# Nome do arquivo com base na data
log_filename = datetime.now().strftime("log_%Y-%m-%d.txt")
log_path = os.path.join(log_dir, log_filename)

# Configurando o logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Substituindo print por logging.info
log = logging.info
url = "https://api.desk.ms/Login/autenticar"

headers = {
    "Authorization": "30c55b0282a7962061dd41a654b6610d02635ddf",
    "JsonPath": "true"
}

payload = {
    "PublicKey": "1bb099a1915916de10c9be05ff4d2cafed607e7f"
}

try:
    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        response_data = response.json()
        token = response_data["access_token"]
        log(f"Autenticação realizada com sucesso! Token: {token}")

    else:
        log(f"Erro na autenticação. Código: {response.status_code}")
        log(f"Mensagem: {response.text}")

except Exception as e:
    log(f"Ocorreu um erro durante a autenticação: {e}")








url = "https://api.desk.ms/ChamadosSuporte/lista"

headers = {
    "Authorization" : f"{token}"
}

payload = {
  "Pesquisa":"CSN - PEDIDO COMPLEMENTAR",
  "Tatual":"",
  "Ativo":"000006",
  "StatusSLA":"",
  "Colunas":
  {
	"Chave":"on",
	"CodChamado":"on",
	"NomePrioridade":"on",
	"DataCriacao":"on",
	"HoraCriacao":"on",
	"DataFinalizacao":"on",
	"HoraFinalizacao":"on",
	"DataAlteracao":"on",
	"HoraAlteracao":"on",
	"NomeStatus":"on",
	"Assunto":"on",
	"Descricao":"on",
	"ChaveUsuario":"on",
	"NomeUsuario":"on",
	"SobrenomeUsuario":"on",
  "NomeCompletoSolicitante":"on",
	"SolicitanteEmail":"on",
	"NomeOperador":"on",
	"SobrenomeOperador":"on",
	"TotalAcoes":"on",
	"TotalAnexos":"on",
	"Sla":"on",
	"CodGrupo":"on",
	"NomeGrupo":"on",
	"CodSolicitacao":"on",
	"CodSubCategoria":"on",
	"CodTipoOcorrencia":"on",
	"CodCategoriaTipo":"on",
	"CodPrioridadeAtual":"on",
	"CodStatusAtual":"on",
	"_6313":"on"
  },
  "Ordem": [
    {
      "Coluna": "Chave",
      "Direcao": "true"
    }
  ]
}



try:
    response = requests.post(url, json=payload, headers=headers)

    if response.status_code == 200:
        response_data = response.json()
        chamados = response_data["root"]

    for chamado in chamados:





        # ---------extraindo as chaves da descrição e adicionando ao txt
        chaves = chamado["Descricao"]
        padrao = r"\b\d{44}\b"
        chaves_nf = re.findall(padrao, chaves)


        # ---------------------------------------------------

        # ----------conseguir o número da filial
        regex_filial = r"\d+"
        filial = chamado["NomeUsuario"]
        cod_chamado = chamado["CodChamado"]
        log(f"Chamado: {cod_chamado}")
        match = re.search(regex_filial, filial)
        if match:
            num_filial = int(match.group())
            log(f"Filial identificada: {num_filial}")
        else:
            log("Não foi possível identificar a filial.")
            continue



        #faz a consulta das notas na central e retorna as notas com pedidos


        notas_com_pedido, notas_sem_pedido, notas_nao_encontradas, notas_nao_loja, notas_gerado_pedido= fun.consultar_pedidos_notas(num_filial, chaves_nf, num_filial)


        fun.interagir_chamado(cod_chamado, token, notas_com_pedido, notas_sem_pedido, notas_nao_encontradas, notas_nao_loja, notas_gerado_pedido)


    else:
        log(f"Erro na requisição. Código: {response.status_code}")
        log(f"Mensagem: {response.text}")

except Exception as e:
    log(f"Ocorreu um erro durante a requisição: {e}")



