# Coleta_Zabbix
 Este script Python utiliza a API do Zabbix 7.0 para coletar dados de monitoramento. Ele realiza a autenticação via API, obtendo o token de acesso, e permite a extração de informações como hosts, itens e triggers monitorados. Para começar, configure suas credenciais de API e o endpoint do Zabbix. O script utiliza a biblioteca requests para fazer chamadas HTTP e processa as respostas JSON retornadas pela API.

# Import_eventszabbix.py
Este script automatiza a coleta e o processamento de eventos de gatilhos do Zabbix para análise. Ele acessa a API do Zabbix para recuperar eventos recentes, classifica-os, e armazena os dados em um arquivo CSV.

Principais funcionalidades:

Autenticação: Conecta à API usando URL e token.
Coleta de dados: Obtém eventos de gatilho (triggers), filtra e remove eventos irrelevantes.
Classificação: Classifica eventos com base no tempo de resolução, determinando se foram resolvidos automaticamente ou geraram um incidente.
Manipulação de dados: Insere novas linhas no arquivo CSV, evitando duplicação, e cria logs de atualização para monitorar o processo.
Backup: Faz o backup dos arquivos processados.
Conversão de horário: Converte timestamps do formato UTC para o horário de São Paulo (America/Sao_Paulo).
Ele é útil para integrar dados do Zabbix com ferramentas de visualização como Power BI, mantendo os dados atualizados e organizados automaticamente.
