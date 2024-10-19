import requests
from datetime import datetime, timedelta
import pandas as pd
import os
import shutil
import csv
from colorama import init, Fore
from pytz import timezone, utc

# Inicializa o colorama
init(autoreset=True)


# Função para classificar o evento com base na diferença de tempo
def classificar_evento(time_diff):
    if time_diff < 1200:  # 20 minutos = 1200 segundos
        return "Resolvido Automaticamente"
    else:
        return "Gerado Incidente"


# Função para extrair a informação da equipe da coluna 'tags'
def get_team_value(tags_list):
    for tag in tags_list:
        if "Equipe" in tag["tag"]:
            return tag["value"]
    return None


# Função para extrair o valor depois de 'name': da coluna 'hosts'
def get_name_from_hosts(hosts_list):
    names = []
    for host in hosts_list:
        if "name" in host:
            names.append(host["name"])
    return names[0] if names else None


# Função para verificar se a linha já existe no DataFrame
def linha_existe(df_existing, eventid):
    return df_existing["eventid"].isin([eventid]).any()


# Função para inserir as linhas no DataFrame e contar a quantidade de inserções
def inserir_linhas(df_existing, df_new):
    count = 0
    rows_to_add = []

    for _, row in df_new.iterrows():
        if not linha_existe(df_existing, row["eventid"]):
            rows_to_add.append(row)
            count += 1

    if rows_to_add:
        df_to_add = pd.DataFrame(rows_to_add)
        df_existing = pd.concat([df_existing, df_to_add], ignore_index=True)
        print(f"Total de linhas inseridas: {count:02d}")

    return df_existing


# Função para criar o log de atualização
def criar_log_atualizacao(log_path, total_eventos, novos_eventos):
    # Obtém a data e hora atual no formato desejado
    data_atualizacao = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    # Cria um dicionário com os dados do log
    log_data = {
        "Ultima_atualizacao_do_Banco": data_atualizacao,
        "Total_de_Eventos_no_Banco": total_eventos,
        "Eventos_Novos_Inseridos": novos_eventos,
    }

    # Verifica se o arquivo de log já existe
    arquivo_existe = os.path.exists(log_path)

    # Adiciona os dados do log ao arquivo CSV
    with open(log_path, "a") as file:
        writer = csv.DictWriter(file, fieldnames=log_data.keys())
        if not arquivo_existe:
            writer.writeheader()  # Escreve os cabeçalhos apenas se o arquivo for novo
        writer.writerow(log_data)


# Função para fazer backup do arquivo
def fazer_backup_arquivo(origem, destino):
    try:
        # Copia o arquivo de origem para o destino
        shutil.copy(origem, destino)
        print(f"{Fore.GREEN}Arquivo de backup criado com sucesso em: {destino}")
    except Exception as e:
        print(f"{Fore.RED}Erro ao fazer backup do arquivo: {e}")


# Função para obter as datas de resolução para múltiplos r_eventids
def obter_datas_resolucao(url, token, r_eventids):
    resolution_payload = {
        "jsonrpc": "2.0",
        "method": "event.get",
        "params": {"output": ["eventid", "clock"], "eventids": r_eventids},
        "auth": token,
        "id": 3,
    }
    response = requests.post(url, json=resolution_payload, timeout=300)
    return {event["eventid"]: event["clock"] for event in response.json()["result"]}


# Função principal para obter eventos de gatilho
def obter_eventos_gatilho(url, token):
    # Obtém a data e hora atuais
    now = datetime.now()

    # Define a data e hora padrão como 24 horas atrás
    default_time_from = now - timedelta(hours=24)

    # Carregar o log de atualização para obter a última data e hora de coleta
    log_dir = "/caminho/powerbi"
    log_path = os.path.join(log_dir, "update_log.csv")

    if os.path.exists(log_path):
        df_log = pd.read_csv(log_path)
        if not df_log.empty:
            ultima_atualizacao = df_log.iloc[-1]["Ultima_atualizacao_do_Banco"]
            time_from = datetime.strptime(ultima_atualizacao, "%d/%m/%Y %H:%M:%S")
        else:
            # Se o arquivo de log estiver vazio, use a data e hora padrão
            time_from = default_time_from
    else:
        # Se o arquivo de log não existir, use a data e hora padrão
        time_from = default_time_from

    time_till = now.timestamp()  # Use o momento atual como o tempo final

    trigger_payload = {
        "jsonrpc": "2.0",
        "method": "event.get",
        "params": {
            "output": [
                "eventid",
                "clock",
                "name",
                "severity",
                "tags",
                "r_eventid",
                "value",
                "hosts",
            ],
            "select_acknowledges": "extend",
            "selectTags": "extend",
            "selectHosts": ["hostid", "name"],
            "selectSuppressionData": "extend",
            "time_from": int(time_from.timestamp()),
            "time_till": int(time_till),
            "sortfield": ["clock", "eventid"],
            "sortorder": "DESC",
        },
        "auth": token,
        "id": 1,
    }

    try:
        response = requests.post(url, json=trigger_payload, timeout=300)
        response.raise_for_status()

        if response.status_code == 200:
            data = response.json().get("result", [])
            if data:
                df_eventos_gatilho = pd.DataFrame(data)

                # Convertendo 'clock' para inteiro
                df_eventos_gatilho["clock"] = df_eventos_gatilho["clock"].astype(int)

                # Filtrar e converter a coluna 'value'
                df_eventos_gatilho["value"] = df_eventos_gatilho["value"].astype(int)
                df_eventos_gatilho = df_eventos_gatilho[
                    df_eventos_gatilho["value"] != 0
                ]

                # Filtrar eventos com base em frases para ignorar na coluna 'name'
                frases_para_ignorar = [
                    r"^Tempo médio - \[/",
                    r"^Quantidade de erros do cliente na uri \[/",
                ]
                regex_para_ignorar = "|".join(frases_para_ignorar)
                df_eventos_gatilho = df_eventos_gatilho[
                    ~df_eventos_gatilho["name"].str.contains(
                        regex_para_ignorar, na=False, regex=True
                    )
                ]

                # Extrair e adicionar a informação da equipe ao DataFrame
                df_eventos_gatilho["Equipe"] = df_eventos_gatilho["tags"].apply(
                    get_team_value
                )
                df_eventos_gatilho.drop(columns=["tags"], inplace=True)

                # Extrair o nome do host
                df_eventos_gatilho["Host_Name"] = df_eventos_gatilho["hosts"].apply(
                    get_name_from_hosts
                )
                df_eventos_gatilho.drop(columns=["hosts"], inplace=True)

                # Formatando a coluna 'clock'
                df_eventos_gatilho["Inicio_Evento"] = df_eventos_gatilho["clock"].apply(
                    lambda x: utc_to_sao_paulo(x)
                )

                # Calcular a diferença de tempo em segundos
                df_eventos_gatilho["Date_Time"] = pd.to_datetime(
                    df_eventos_gatilho["Inicio_Evento"], format="%d/%m/%Y %H:%M:%S"
                )
                df_eventos_gatilho["Time_Difference"] = (
                    datetime.now() - df_eventos_gatilho["Date_Time"]
                ).dt.total_seconds()

                # Classificar os eventos
                df_eventos_gatilho["Classificação_Evento"] = df_eventos_gatilho[
                    "Time_Difference"
                ].apply(classificar_evento)

                # Remover linhas duplicadas com base na coluna 'eventid'
                df_eventos_gatilho = df_eventos_gatilho.drop_duplicates(
                    subset=["eventid"], keep="first"
                )

                # Carregar o log de atualização
                log_path = "caminho/update_log.csv"

                # Verificando a quantidade de eventos novos inseridos
                if os.path.exists(log_path):
                    df_log = pd.read_csv(log_path)
                    if (
                        "Total_de_Eventos_no_Banco" in df_log.columns
                    ):  # Verifica se a coluna existe
                        eventos_anteriores = df_log.iloc[-1][
                            "Total_de_Eventos_no_Banco"
                        ]
                        novos_eventos = len(df_eventos_gatilho) - eventos_anteriores
                    else:
                        eventos_anteriores = 0
                        novos_eventos = len(df_eventos_gatilho)
                else:
                    novos_eventos = len(df_eventos_gatilho)

                criar_log_atualizacao(log_path, len(df_eventos_gatilho), novos_eventos)

                # Adicione aqui os r_eventids que você quer buscar as datas de resolução
                r_eventids = df_eventos_gatilho["r_eventid"].tolist()
                resolution_times = obter_datas_resolucao(url, token, r_eventids)

                # Inserir as datas de resolução no DataFrame
                df_eventos_gatilho["Data_Resolução"] = df_eventos_gatilho[
                    "r_eventid"
                ].map(resolution_times)

                # Convertendo 'Data_Resolução' para horário de São Paulo
                df_eventos_gatilho["Fim_Evento"] = df_eventos_gatilho[
                    "Data_Resolução"
                ].apply(lambda x: utc_to_sao_paulo(int(x)) if pd.notnull(x) else None)

                # Reordenar as colunas
                df_eventos_gatilho = df_eventos_gatilho[
                    [
                        "eventid",
                        "Inicio_Evento",
                        "Fim_Evento",
                        "clock",
                        "name",
                        "severity",
                        "Date_Time",
                        "Classificação_Evento",
                        "value",
                        "Equipe",
                        "Host_Name",
                    ]
                ]

                # Carregar ou criar o arquivo CSV
                csv_path = "caminho/nomedoarquivo.csv"
                if os.path.exists(csv_path):
                    df_existing = pd.read_csv(csv_path, dtype={"eventid": str})
                    df_existing = inserir_linhas(df_existing, df_eventos_gatilho)
                    df_existing.to_csv(csv_path, index=False)
                else:
                    df_eventos_gatilho.to_csv(csv_path, index=False)

                return df_eventos_gatilho[
                    [
                        "eventid",
                        "Inicio_Evento",
                        "Fim_Evento",
                        "clock",
                        "name",
                        "severity",
                        "Date_Time",
                        "Classificação_Evento",
                        "value",
                        "Equipe",
                        "Host_Name",
                    ]
                ]
            else:
                print(f"{Fore.RED}Nenhum dado retornado pela API.")
                return pd.DataFrame()
        else:
            print(f"{Fore.RED}Erro na solicitação API: {response.status_code}")
            return pd.DataFrame()
    except requests.RequestException as e:
        print(f"{Fore.RED}Erro ao obter eventos de gatilho: {e}")
        return pd.DataFrame()


# Função para converter o UTC para horário de São Paulo
def utc_to_sao_paulo(utc_time):
    try:
        utc_dt = utc.localize(datetime.utcfromtimestamp(int(utc_time)))
        sao_paulo_tz = timezone("America/Sao_Paulo")
        sao_paulo_dt = utc_dt.astimezone(sao_paulo_tz)
        return sao_paulo_dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception as e:
        print(f"Erro ao converter UTC para São Paulo: {e}")
        return None


# URL e token de autenticação
if __name__ == "__main__":
    url = "SEU_URL_AQUI"
    token = "SEU_TOKEN_AQUI"

    df_eventos_gatilho = obter_eventos_gatilho(url, token)

    if not df_eventos_gatilho.empty:
        current_dir = os.getcwd()
        csvURL_path = os.path.join(current_dir, "zabbixeventsout.csv")
        df_eventos_gatilho.to_csv(csvURL_path, index=False)
        print(f"{Fore.BLUE}Resultados salvos em '{csvURL_path}'.")
