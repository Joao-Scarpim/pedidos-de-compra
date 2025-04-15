import pyodbc
import os
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime

# === CONFIGURAÇÃO DO LOG ===
log_dir = "logs_pedidos_de_compras"
os.makedirs(log_dir, exist_ok=True)
log_filename = datetime.now().strftime("log_%Y-%m-%d.txt")
log_path = os.path.join(log_dir, log_filename)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.info

load_dotenv()


awayson_db_config = {
    "server": os.getenv("AWAYSON_DB_SERVER"),
    "database": os.getenv("AWAYSON_DB_DATABASE"),
    "username": os.getenv("AWAYSON_DB_USER"),
    "password": os.getenv("AWAYSON_DB_PASS")
}

central_db_config = {
    "server": os.getenv("CENTRAL_DB_SERVER"),
    "database": os.getenv("CENTRAL_DB_DATABASE"),
    "username": os.getenv("CENTRAL_DB_USER"),
    "password": os.getenv("CENTRAL_DB_PASS")
}

def obter_ip_filial(filial):
    if 1 <= filial <= 200 or filial == 241:
        ip = f"10.16.{filial}.24"
    elif 201 <= filial <= 299:
        ip = f"10.17.{filial % 100}.24"
    elif 300 <= filial <= 399:
        ip = f"10.17.1{filial % 100}.24"
    elif 400 <= filial <= 499:
        ip = f"10.18.{filial % 100}.24"
    elif filial == 247:
        ip = f"192.168.201.1"
    else:
        raise ValueError("Número de filial inválido.")

    filial_db_config = {
        "server": ip,
        "database": os.getenv("FILIAL_DB_DATABASE"),
        "username": os.getenv("FILIAL_DB_USER"),
        "password": os.getenv("FILIAL_DB_PASS")
    }

    return filial_db_config


def conectar_filial(num_filial):
    """Estabelece conexão com o banco de dados central e retorna a conexão."""

    config_bd_filial = obter_ip_filial(num_filial)
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={config_bd_filial['server']};"
            f"DATABASE={config_bd_filial['database']};"
            f"UID={config_bd_filial['username']};"
            f"PWD={config_bd_filial['password']}"
        )
        return conn
    except Exception as e:
        log(f"Erro ao conectar ao banco da filial: {e}")
        return None



def conectar_awayson():
    """Estabelece conexão com o banco de dados awayson e retorna a conexão."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={awayson_db_config['server']};"
            f"DATABASE={awayson_db_config['database']};"
            f"UID={awayson_db_config['username']};"
            f"PWD={awayson_db_config['password']}"
        )
        return conn

    except Exception as e:
        log(f"Erro ao conectar ao banco awayson: {e}")
        return None

def conectar_central():
    """Estabelece conexão com o banco de dados central e retorna a conexão."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={central_db_config['server']};"
            f"DATABASE={central_db_config['database']};"
            f"UID={central_db_config['username']};"
            f"PWD={central_db_config['password']}"
        )
        return conn

    except Exception as e:
        log(f"Erro ao conectar ao banco central: {e}")
        return None




def ler_arquivo(nome_arquivo):
    """Lê um arquivo linha por linha e retorna uma lista de strings sem quebras de linha."""
    if os.path.exists(nome_arquivo):
        with open(nome_arquivo, "r") as arquivo:
            return [linha.strip() for linha in arquivo.readlines() if linha.strip()]
    return []


def is_pepsico(entidade):
    entidades_pepsico = {
        '5362', '2027', '5660', '2025', '2026', '3436', '5051', '5316', '5902',
        '6550', '7192', '7269', '7706', '7840', '7842', '8791', '11062', '12509',
        '13220', '18691', '4564598', '8497914', '8524792', '8535855', '8580695',
        '12293770', '12790279', '13364893', '13366195', '13367260', '14515766',
        '14515817', '2945'
    }
    return str(entidade) in entidades_pepsico



def gerar_pedido_pepsico(chave_nfe, empresa):
    try:
        with open("gerar_pedido_pepsico.sql", "r", encoding="utf-8") as f:
            sql_script = f.read()

        # Prefixo com as declarações das variáveis
        prefixo = f"""
        DECLARE @EMPRESA NUMERIC(10) = {empresa}
        DECLARE @CHAVE_NFE VARCHAR(50) = '{chave_nfe}'
        """

        # Junta as variáveis com o script real
        sql_completo = prefixo + "\n" + sql_script

        conn_central = conectar_central()
        cursor = conn_central.cursor()

        # Executa o script de geração de pedido
        cursor.execute(sql_completo)
        conn_central.commit()

        log(f"Pedido gerado com sucesso para a nota {chave_nfe}.")

        # Consulta o pedido gerado
        consulta = """
        SELECT PEDIDO_COMPRA 
        FROM NF_COMPRA 
        WHERE CHAVE_NFE = ?
          AND EMPRESA = ?
        """
        cursor.execute(consulta, (chave_nfe, empresa))
        resultado = cursor.fetchone()

        if resultado and resultado[0]:
            log(f"Pedido de compra gerado: {resultado[0]}")
        else:
            log("Nenhum pedido de compra foi associado à nota.")

        cursor.close()

    except Exception as e:
        log(f"Erro ao gerar ou consultar pedido para a nota {chave_nfe}: {e}")







def consultar_pedidos_notas(num_filial, chaves, empresa):
    notas_com_pedido = []
    notas_nao_central = []
    notas_sem_pedido = []
    notas_nao_loja = []
    notas_gerado_pedido = []

    try:
        conn = conectar_awayson()
        if conn is None:
            return notas_com_pedido, notas_sem_pedido, notas_nao_central, notas_nao_loja

        cursor = conn.cursor()

        for chave in chaves:
            cursor.execute('''SELECT A.NF_COMPRA, A.PEDIDO_COMPRA, B.NOME, A.ENTIDADE, A.EMPRESA
                              FROM NF_COMPRA AS A
                              JOIN ENTIDADES AS B ON A.ENTIDADE = B.ENTIDADE
                              WHERE CHAVE_NFE = ?''', (chave,))
            resultado = cursor.fetchone()

            if resultado:
                nf_compra, pedido_compra, nome, entidade, empresa_nota = resultado
                nota_info = {"CHAVE": chave, "ENTIDADE": str(entidade), "NOME": nome, "EMPRESA": empresa_nota}

                if pedido_compra is None:
                    # Geração do pedido para notas PEPSICO sem pedido

                    gerar_pedido_pepsico(chave, empresa)
                    # Após tentar gerar, vamos checar de novo se gerou
                    cursor.execute("SELECT PEDIDO_COMPRA FROM NF_COMPRA WHERE CHAVE_NFE = ?", (chave,))
                    pedido_check = cursor.fetchone()
                    if pedido_check and pedido_check[0]:
                        notas_gerado_pedido.append(nota_info)
                    else:
                        notas_sem_pedido.append(nota_info)

                else:
                    notas_com_pedido.append(nota_info)
            else:
                notas_nao_central.append(chave)

        cursor.close()
        conn.close()

    except Exception as e:
        log(f"Erro ao consultar notas na central: {e}")
        return notas_com_pedido, notas_sem_pedido, notas_nao_central, notas_nao_loja

    # Verifica se essas notas com pedido também estão na loja
    try:
        conn_filial = conectar_filial(num_filial)
        if conn_filial is None:
            return notas_com_pedido, notas_sem_pedido, notas_nao_central, notas_nao_loja

        cursor = conn_filial.cursor()

        for nota in notas_com_pedido:
            cursor.execute("SELECT NF_COMPRA FROM NF_COMPRA WHERE CHAVE_NFE = ?", (nota["CHAVE"],))
            resultado = cursor.fetchone()

            if not resultado:
                notas_nao_loja.append(nota)

        cursor.close()
        conn_filial.close()

    except Exception as e:
        log(f"Erro ao consultar notas na filial: {e}")

    return notas_com_pedido, notas_sem_pedido, notas_nao_central, notas_nao_loja, notas_gerado_pedido






def interagir_chamado(cod_chamado, token, notas_com_pedido, notas_sem_pedido, notas_nao_encontradas, notas_nao_loja, notas_gerado_pedido):
    # Criando a descrição formatada
    descricao = "Resumo da Validação das notas\n\n"

    # Conjunto de entidades da PEPSICO
    entidades_pepsico = {
        '5362', '2027', '5660', '2025', '2026', '3436', '5051', '5316', '5902',
        '6550', '7192', '7269', '7706', '7840', '7842', '8791', '11062', '12509',
        '13220', '18691', '4564598', '8497914', '8524792', '8535855', '8580695',
        '12293770', '12790279', '13364893', '13366195', '13367260', '14515766',
        '14515817', '2945'
    }

    # Verifica se existe nota da PEPSICO em qualquer das listas
    existe_nota_pepsico = any(
        nota.get("ENTIDADE") in entidades_pepsico
        for nota in notas_com_pedido + notas_sem_pedido
    )



    # Adiciona blocos de descrição por tipo
    if notas_com_pedido:
        descricao += "*Notas com Pedido de Compra:*\n\n"
        for nota in notas_com_pedido:
            descricao += f"{nota['CHAVE']} -- {nota['NOME']}\n"
        descricao += "\n"

    if notas_sem_pedido:
        descricao += "*Notas sem Pedido de Compra:*\n"
        for nota in notas_sem_pedido:
            descricao += f"{nota['CHAVE']} -- {nota['NOME']}\n"

    if notas_nao_encontradas:
        descricao += "*Notas não encontradas na Central:*\n"
        descricao += "\n".join(notas_nao_encontradas) + "\n\n"

    if notas_nao_loja:
        descricao +="*Notas não encontradas na loja:*\n"
        for nota in notas_nao_loja:
            descricao += f"{nota['CHAVE']} -- {nota['NOME']}\n"
    if notas_gerado_pedido:
        descricao +="*Notas com pedidos gerados, segue para recebimento:*\n"
        for nota in notas_gerado_pedido:
            descricao += f"{nota['CHAVE']} -- {nota['NOME']}\n"
    # Definição do status do chamado
    if notas_nao_encontradas or notas_sem_pedido:
        descricao += "Chamado encaminhado para análise, favor aguardar.\n\n"
        cod_status = "0000006"  # Chamado permanece aberto
    else:
        cod_status = "0000002"  # Chamado pode ser encerrado

    # Data da interação
    data_interacao = datetime.now().strftime("%d-%m-%Y")

    # Payload da API
    payload = {
        "Chave": cod_chamado,
        "TChamado": {
            "CodFormaAtendimento": "1",
            "CodStatus": cod_status,
            "CodAprovador": [""],
            "TransferirOperador": "",
            "TransferirGrupo": "",
            "CodTerceiros": "",
            "Protocolo": "",
            "Descricao": descricao,
            "CodAgendamento": "",
            "DataAgendamento": "",
            "HoraAgendamento": "",
            "CodCausa": "000467",
            "CodOperador": "249",
            "CodGrupo": "",
            "EnviarEmail": "S",
            "EnvBase": "N",
            "CodFPMsg": "",
            "DataInteracao": data_interacao,
            "HoraInicial": "",
            "HoraFinal": "",
            "SMS": "",
            "ObservacaoInterna": "",
            "PrimeiroAtendimento": "S",
            "SegundoAtendimento": "N"
        },
        "TIc": {
            "Chave": {
                "278": "on",
                "280": "on"
            }
        }
    }

    headers = {
        "Authorization": token,
        "Content-Type": "application/json"
    }

    try:
        response = requests.put("https://api.desk.ms/ChamadosSuporte/interagir", json=payload, headers=headers)
        if response.status_code == 200:
            if cod_status == "0000006":
                log(f"Chamado {cod_chamado} encaminhado para análise. \n")
            if cod_status == "0000002":
                log(f"Chamado {cod_chamado} encerrado com sucesso! \n")
        else:
            log(f"Erro ao interagir no chamado. Código: {response.status_code}")
            log("Resposta da API:")
            log(response.text)
            try:
                log("Detalhes do erro:", response.json())
            except ValueError:
                log("Não foi possível converter a resposta da API para JSON.")
    except requests.exceptions.RequestException as e:
        log(f"Erro ao conectar com a API: {e}")