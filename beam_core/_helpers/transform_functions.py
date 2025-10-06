# Copyright 2025 TecOnca Data Solutions.


import logging
from datetime import datetime

def formatar_data(valor_data: str, formato_entrada: str, formato_saida: str) -> str | None:
    # Se o valor de entrada não for uma string ou estiver vazio, retorna None
    if not isinstance(valor_data, str) or not valor_data:
        return None

    if 'Timestamp' in valor_data:
        try:
            numero_em_string = valor_data[10:-1]
            timestamp_numerico = float(numero_em_string)
            objeto_data = datetime.fromtimestamp(timestamp_numerico)
            # Formata o objeto datetime para a string de saída
            return objeto_data.strftime(formato_saida)
        except (ValueError, TypeError) as e:
            logging.warning(f"Não foi possível formatar a data '{valor_data}' com o formato de entrada '{formato_entrada}'. Erro: {e}")
            return None # Retorna None em caso de erro
    else:

        # Analisa a string para um objeto datetime
        # %Y - Ano com 4 dígitos
        # %m - Mês com 2 dígitos
        # %d - Dia do mês com 2 dígitos
        # %H - Hora (00-23)
        # %M - Minuto (00-59)
        # %S - Segundo (00-59)
        # %f - Microssegundo
        # %Z - Nome do fuso horário
        try:
            objeto_datetime = datetime.strptime(valor_data, formato_entrada)
            data_final_string = objeto_datetime.strftime(formato_saida)
            return data_final_string
        except (ValueError, TypeError) as e:
            logging.warning(f"Não foi possível formatar a data '{valor_data}' com o formato de entrada '{formato_entrada}'. Erro: {e}")
            return None

def generic_transform(row_dict):
    return row_dict

def transform_aluno_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ALU_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('ALU_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['ALU_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ALU_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_aluno_alu_deficiencias_pcd_table(row_dict):
    transformed_row = row_dict.copy()
        

    return transformed_row

def transform_aluno_teste_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ALT_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('ALT_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['ALT_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ALT_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_aluno_teste_resposta_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ATR_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ATR_DT_CRIACAO'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_aluno_teste_resposta_historico_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ATH_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ATH_DT_CRIACAO'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_ano_letivo_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ANO_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('ANO_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['ANO_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ANO_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_area_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ARE_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('ARE_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['ARE_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ARE_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_arquivo_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ARQ_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('ARQ_DT_ATUALIZACAO'),
        formato_entrada='',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['ARQ_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ARQ_DT_CRIACAO'),
        formato_entrada='',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_avaliacao_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['AVA_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('AVA_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['AVA_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('AVA_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_avaliacao_municipio_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['AVM_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('AVM_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['AVM_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('AVM_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['AVM_DT_DISPONIVEL'] = formatar_data(
        valor_data=transformed_row.get('AVM_DT_DISPONIVEL'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['AVM_DT_FIM'] = formatar_data(
        valor_data=transformed_row.get('AVM_DT_FIM'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['AVM_DT_INICIO'] = formatar_data(
        valor_data=transformed_row.get('AVM_DT_INICIO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_avaliacao_online_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_avaliacao_online_page_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_avaliacao_online_question_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_avaliacao_online_question_alternative_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_avaliacao_teste_table(row_dict):
    transformed_row = row_dict.copy()
        

    return transformed_row

def transform_dados_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['DAT_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('DAT_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['DAT_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('DAT_DT_CRIACAO'),
        formato_entrada='',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_disciplina_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['DIS_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('DIS_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['DIS_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('DIS_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_envios_tutor_mensagens_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_escola_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ESC_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('ESC_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['ESC_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ESC_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_estados_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_external_reports_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_forget_password_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_formacao_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['FOR_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('FOR_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['FOR_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('FOR_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_genero_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['GEN_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('GEN_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['GEN_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('GEN_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_importar_dados_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['DAT_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('DAT_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['DAT_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('DAT_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_infrequencia_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['IFR_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('IFR_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['IFR_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('IFR_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_job_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['endDate'] = formatar_data(
        valor_data=transformed_row.get('endDate'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['startDate'] = formatar_data(
        valor_data=transformed_row.get('startDate'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_matriz_referencia_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['MAR_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('MAR_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['MAR_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('MAR_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_matriz_referencia_serie_table(row_dict):
    transformed_row = row_dict.copy()
        

    return transformed_row

def transform_matriz_referencia_topico_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['MTO_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('MTO_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['MTO_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('MTO_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_matriz_referencia_topico_items_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['MTI_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('MTI_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['MTI_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('MTI_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_messages_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['MEN_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('MEN_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['MEN_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('MEN_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_messages_municipios_municipio_table(row_dict):
    transformed_row = row_dict.copy()
        

    return transformed_row

def transform_messages_schools_escola_table(row_dict):
    transformed_row = row_dict.copy()
        

    return transformed_row

def transform_microdata_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_migrations_table(row_dict):
    transformed_row = row_dict.copy()
        

    return transformed_row

def transform_municipio_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['MUN_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('MUN_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['MUN_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('MUN_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['MUN_DT_FIM'] = formatar_data(
        valor_data=transformed_row.get('MUN_DT_FIM'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['MUN_DT_INICIO'] = formatar_data(
        valor_data=transformed_row.get('MUN_DT_INICIO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_notifications_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updateAt'] = formatar_data(
        valor_data=transformed_row.get('updateAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_pcd_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['PCD_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('PCD_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['PCD_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('PCD_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_perfil_base_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['PER_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('PER_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['PER_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('PER_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_professor_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['PRO_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('PRO_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['PRO_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('PRO_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['PRO_DT_NASC'] = formatar_data(
        valor_data=transformed_row.get('PRO_DT_NASC'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_raca_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['PEL_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('PEL_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['PEL_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('PEL_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_regionais_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_descriptor_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_edition_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_not_evaluated_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_question_table(row_dict):
    transformed_row = row_dict.copy()
        

    return transformed_row

def transform_report_question_option_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_race_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_subject_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_series_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['SER_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('SER_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['SER_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('SER_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_sub_perfil_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['SPE_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('SPE_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['SPE_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('SPE_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_sub_perfil_area_table(row_dict):
    transformed_row = row_dict.copy()
        

    return transformed_row

def transform_system_logs_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_templates_mensagens_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_teste_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['TES_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('TES_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['TES_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('TES_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_teste_gabarito_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['TEG_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('TEG_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['TEG_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('TEG_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_transferencia_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['TRF_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('TRF_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['TRF_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('TRF_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_turma_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['TUR_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('TUR_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['TUR_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('TUR_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_turma_aluno_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['endDate'] = formatar_data(
        valor_data=transformed_row.get('endDate'),
        formato_entrada='%Y-%m-%d',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['startDate'] = formatar_data(
        valor_data=transformed_row.get('startDate'),
        formato_entrada='%Y-%m-%d',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_turma_professor_table(row_dict):
    transformed_row = row_dict.copy()
        

    return transformed_row

def transform_tutor_mensagens_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_usuario_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['USU_DT_ATUALIZACAO'] = formatar_data(
        valor_data=transformed_row.get('USU_DT_ATUALIZACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['USU_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('USU_DT_CRIACAO'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

TRANSFORM_MAPPING = {
    'aluno': transform_aluno_table,
    'aluno_alu_deficiencias_pcd': transform_aluno_alu_deficiencias_pcd_table,
    'aluno_teste': transform_aluno_teste_table,
    'aluno_teste_resposta': transform_aluno_teste_resposta_table,
    'aluno_teste_resposta_historico': transform_aluno_teste_resposta_historico_table,
    'ano_letivo': transform_ano_letivo_table,
    'area': transform_area_table,
    'arquivo': transform_arquivo_table,
    'avaliacao': transform_avaliacao_table,
    'avaliacao_municipio': transform_avaliacao_municipio_table,
    'avaliacao_online': transform_avaliacao_online_table,
    'avaliacao_online_page': transform_avaliacao_online_page_table,
    'avaliacao_online_question': transform_avaliacao_online_question_table,
    'avaliacao_online_question_alternative': transform_avaliacao_online_question_alternative_table,
    'avaliacao_teste': transform_avaliacao_teste_table,
    'dados': transform_dados_table,
    'disciplina': transform_disciplina_table,
    'envios_tutor_mensagens': transform_envios_tutor_mensagens_table,
    'escola': transform_escola_table,
    'estados': transform_estados_table,
    'external_reports': transform_external_reports_table,
    'forget_password': transform_forget_password_table,
    'formacao': transform_formacao_table,
    'genero': transform_genero_table,
    'importar_dados': transform_importar_dados_table,
    'infrequencia': transform_infrequencia_table,
    'job': transform_job_table,
    'matriz_referencia': transform_matriz_referencia_table,
    'matriz_referencia_serie': transform_matriz_referencia_serie_table,
    'matriz_referencia_topico': transform_matriz_referencia_topico_table,
    'matriz_referencia_topico_items': transform_matriz_referencia_topico_items_table,
    'messages': transform_messages_table,
    'messages_municipios_municipio': transform_messages_municipios_municipio_table,
    'messages_schools_escola': transform_messages_schools_escola_table,
    'microdata': transform_microdata_table,
    'migrations': transform_migrations_table,
    'municipio': transform_municipio_table,
    'notifications': transform_notifications_table,
    'pcd': transform_pcd_table,
    'perfil_base': transform_perfil_base_table,
    'professor': transform_professor_table,
    'raca': transform_raca_table,
    'regionais': transform_regionais_table,
    'report_descriptor': transform_report_descriptor_table,
    'report_edition': transform_report_edition_table,
    'report_not_evaluated': transform_report_not_evaluated_table,
    'report_question': transform_report_question_table,
    'report_question_option': transform_report_question_option_table,
    'report_race': transform_report_race_table,
    'report_subject': transform_report_subject_table,
    'series': transform_series_table,
    'sub_perfil': transform_sub_perfil_table,
    'sub_perfil_area': transform_sub_perfil_area_table,
    'system_logs': transform_system_logs_table,
    'templates_mensagens': transform_templates_mensagens_table,
    'teste': transform_teste_table,
    'teste_gabarito': transform_teste_gabarito_table,
    'transferencia': transform_transferencia_table,
    'turma': transform_turma_table,
    'turma_aluno': transform_turma_aluno_table,
    'turma_professor': transform_turma_professor_table,
    'tutor_mensagens': transform_tutor_mensagens_table,
    'usuario': transform_usuario_table,
    }
