# Copyright 2025 TecOnca Data Solutions.


import logging
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

def formatar_data(valor_data: str, formato_entrada: str, formato_saida: str) -> str | None:
    # Se o valor de entrada não for uma string ou estiver vazio, retorna None
    if not isinstance(valor_data, str) or not valor_data:
        return None

    # if 'Timestamp' in valor_data:
    #     try:
    #         numero_em_string = valor_data[10:-1]
    #         timestamp_numerico = float(numero_em_string)
    #         objeto_data = datetime.fromtimestamp(timestamp_numerico)
    #         # Formata o objeto datetime para a string de saída
    #         return objeto_data.strftime(formato_saida)
    #     except (ValueError, TypeError) as e:
    #         logging.warning(f"Não foi possível formatar a data '{valor_data}' com o formato de entrada '{formato_entrada}'. Erro: {e}")
    #         return None # Retorna None em caso de erro
    # else:

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
        
    transformed_row['ALU_ATIVO'] = int(transformed_row.get('ALU_ATIVO'))
    transformed_row['ALU_AVATAR'] = str(transformed_row.get('ALU_AVATAR'))
    transformed_row['ALU_BAIRRO'] = str(transformed_row.get('ALU_BAIRRO'))
    transformed_row['ALU_CEP'] = str(transformed_row.get('ALU_CEP'))
    transformed_row['ALU_CIDADE'] = str(transformed_row.get('ALU_CIDADE'))
    transformed_row['ALU_COD'] = int(transformed_row.get('ALU_COD'))
    transformed_row['ALU_COMPLEMENTO'] = str(transformed_row.get('ALU_COMPLEMENTO'))
    transformed_row['ALU_CPF'] = str(transformed_row.get('ALU_CPF'))
    transformed_row['ALU_DEFICIENCIA_BY_IMPORT'] = str(transformed_row.get('ALU_DEFICIENCIA_BY_IMPORT'))
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
    transformed_row['ALU_DT_NASC'] = str(transformed_row.get('ALU_DT_NASC'))
    transformed_row['ALU_EMAIL'] = str(transformed_row.get('ALU_EMAIL'))
    transformed_row['ALU_ENDERECO'] = str(transformed_row.get('ALU_ENDERECO'))
    transformed_row['ALU_ESC_ID'] = int(transformed_row.get('ALU_ESC_ID'))
    transformed_row['ALU_GEN_ID'] = int(transformed_row.get('ALU_GEN_ID'))
    transformed_row['ALU_ID'] = int(transformed_row.get('ALU_ID'))
    transformed_row['ALU_INEP'] = str(transformed_row.get('ALU_INEP'))
    transformed_row['ALU_NOME'] = str(transformed_row.get('ALU_NOME'))
    transformed_row['ALU_NOME_MAE'] = str(transformed_row.get('ALU_NOME_MAE'))
    transformed_row['ALU_NOME_PAI'] = str(transformed_row.get('ALU_NOME_PAI'))
    transformed_row['ALU_NOME_RESP'] = str(transformed_row.get('ALU_NOME_RESP'))
    transformed_row['ALU_NUMERO'] = str(transformed_row.get('ALU_NUMERO'))
    transformed_row['ALU_OLD_ID'] = int(transformed_row.get('ALU_OLD_ID'))
    transformed_row['ALU_PCD_ID'] = int(transformed_row.get('ALU_PCD_ID'))
    transformed_row['ALU_PEL_ID'] = int(transformed_row.get('ALU_PEL_ID'))
    transformed_row['ALU_SER_ID'] = int(transformed_row.get('ALU_SER_ID'))
    transformed_row['ALU_STATUS'] = str(transformed_row.get('ALU_STATUS'))
    transformed_row['ALU_TEL1'] = str(transformed_row.get('ALU_TEL1'))
    transformed_row['ALU_TEL2'] = str(transformed_row.get('ALU_TEL2'))
    transformed_row['ALU_TUR_ID'] = int(transformed_row.get('ALU_TUR_ID'))
    transformed_row['ALU_UF'] = str(transformed_row.get('ALU_UF'))
    transformed_row['ALU_WHATSAPP'] = str(transformed_row.get('ALU_WHATSAPP'))

    return transformed_row

def transform_aluno_alu_deficiencias_pcd_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['alunoALUID'] = int(transformed_row.get('alunoALUID'))
    transformed_row['pcdPCDID'] = int(transformed_row.get('pcdPCDID'))

    return transformed_row

def transform_aluno_teste_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ALT_ALU_ID'] = int(transformed_row.get('ALT_ALU_ID'))
    transformed_row['ALT_ATIVO'] = int(transformed_row.get('ALT_ATIVO'))
    transformed_row['ALT_BY_AVA_ONLINE'] = int(transformed_row.get('ALT_BY_AVA_ONLINE'))
    transformed_row['ALT_BY_EDLER'] = int(transformed_row.get('ALT_BY_EDLER'))
    transformed_row['ALT_BY_HERBY'] = int(transformed_row.get('ALT_BY_HERBY'))
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
    transformed_row['ALT_FINALIZADO'] = int(transformed_row.get('ALT_FINALIZADO'))
    transformed_row['ALT_FORNECEDOR'] = str(transformed_row.get('ALT_FORNECEDOR'))
    transformed_row['ALT_ID'] = int(transformed_row.get('ALT_ID'))
    transformed_row['ALT_JUSTIFICATIVA'] = str(transformed_row.get('ALT_JUSTIFICATIVA'))
    transformed_row['ALT_TES_ID'] = int(transformed_row.get('ALT_TES_ID'))
    transformed_row['ALT_USU_ID'] = int(transformed_row.get('ALT_USU_ID'))
    transformed_row['schoolClassTURID'] = int(transformed_row.get('schoolClassTURID'))

    return transformed_row

def transform_aluno_teste_resposta_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ATR_ALT_ID'] = int(transformed_row.get('ATR_ALT_ID'))
    transformed_row['ATR_CERTO'] = int(transformed_row.get('ATR_CERTO'))
    transformed_row['ATR_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ATR_DT_CRIACAO'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['ATR_ID'] = int(transformed_row.get('ATR_ID'))
    transformed_row['ATR_MTI_ID'] = int(transformed_row.get('ATR_MTI_ID'))
    transformed_row['ATR_RESPOSTA'] = str(transformed_row.get('ATR_RESPOSTA'))
    transformed_row['questionTemplateTEGID'] = int(transformed_row.get('questionTemplateTEGID'))

    return transformed_row

def transform_aluno_teste_resposta_historico_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ATH_ALT_ID'] = int(transformed_row.get('ATH_ALT_ID'))
    transformed_row['ATH_ATR_ID'] = int(transformed_row.get('ATH_ATR_ID'))
    transformed_row['ATH_ATR_RESPOSTA_ANTIGA'] = str(transformed_row.get('ATH_ATR_RESPOSTA_ANTIGA'))
    transformed_row['ATH_ATR_RESPOSTA_NOVA'] = str(transformed_row.get('ATH_ATR_RESPOSTA_NOVA'))
    transformed_row['ATH_DT_CRIACAO'] = formatar_data(
        valor_data=transformed_row.get('ATH_DT_CRIACAO'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['ATH_ID'] = int(transformed_row.get('ATH_ID'))
    transformed_row['ATH_OPERACAO'] = str(transformed_row.get('ATH_OPERACAO'))
    transformed_row['ATH_TEG_ID'] = int(transformed_row.get('ATH_TEG_ID'))

    return transformed_row

def transform_ano_letivo_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ANO_ATIVO'] = int(transformed_row.get('ANO_ATIVO'))
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
    transformed_row['ANO_ID'] = int(transformed_row.get('ANO_ID'))
    transformed_row['ANO_NOME'] = str(transformed_row.get('ANO_NOME'))
    transformed_row['ANO_OLD_ID'] = int(transformed_row.get('ANO_OLD_ID'))

    return transformed_row

def transform_area_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ARE_ATIVO'] = int(transformed_row.get('ARE_ATIVO'))
    transformed_row['ARE_DESCRICAO'] = str(transformed_row.get('ARE_DESCRICAO'))
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
    transformed_row['ARE_ID'] = int(transformed_row.get('ARE_ID'))
    transformed_row['ARE_NOME'] = str(transformed_row.get('ARE_NOME'))

    return transformed_row

def transform_avaliacao_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['AVA_ANO'] = str(transformed_row.get('AVA_ANO'))
    transformed_row['AVA_ATIVO'] = int(transformed_row.get('AVA_ATIVO'))
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
    transformed_row['AVA_ID'] = int(transformed_row.get('AVA_ID'))
    transformed_row['AVA_NOME'] = str(transformed_row.get('AVA_NOME'))
    transformed_row['AVA_OLD_ID'] = int(transformed_row.get('AVA_OLD_ID'))

    return transformed_row

def transform_avaliacao_municipio_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['AVM_ATIVO'] = int(transformed_row.get('AVM_ATIVO'))
    transformed_row['AVM_AVA_ID'] = int(transformed_row.get('AVM_AVA_ID'))
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
    transformed_row['AVM_ID'] = int(transformed_row.get('AVM_ID'))
    transformed_row['AVM_MUN_ID'] = int(transformed_row.get('AVM_MUN_ID'))
    transformed_row['AVM_OLD_ID'] = int(transformed_row.get('AVM_OLD_ID'))
    transformed_row['AVM_TIPO'] = str(transformed_row.get('AVM_TIPO'))

    return transformed_row

def transform_avaliacao_online_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['active'] = int(transformed_row.get('active'))
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_avaliacao_online_page_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['assessmentOnlineId'] = int(transformed_row.get('assessmentOnlineId'))
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['image'] = str(transformed_row.get('image'))
    transformed_row['order'] = int(transformed_row.get('order'))
    transformed_row['title'] = str(transformed_row.get('title'))
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
    transformed_row['description'] = str(transformed_row.get('description'))
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['order'] = int(transformed_row.get('order'))
    transformed_row['pageId'] = int(transformed_row.get('pageId'))
    transformed_row['questionTemplateId'] = int(transformed_row.get('questionTemplateId'))
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
    transformed_row['description'] = str(transformed_row.get('description'))
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['image'] = str(transformed_row.get('image'))
    transformed_row['option'] = str(transformed_row.get('option'))
    transformed_row['questionId'] = int(transformed_row.get('questionId'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_avaliacao_teste_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['AVA_ID'] = int(transformed_row.get('AVA_ID'))
    transformed_row['TES_ID'] = int(transformed_row.get('TES_ID'))

    return transformed_row

def transform_disciplina_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['DIS_ATIVO'] = int(transformed_row.get('DIS_ATIVO'))
    transformed_row['DIS_COLOR'] = str(transformed_row.get('DIS_COLOR'))
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
    transformed_row['DIS_ID'] = int(transformed_row.get('DIS_ID'))
    transformed_row['DIS_NOME'] = str(transformed_row.get('DIS_NOME'))
    transformed_row['DIS_OLD_ID'] = int(transformed_row.get('DIS_OLD_ID'))
    transformed_row['DIS_TIPO'] = str(transformed_row.get('DIS_TIPO'))

    return transformed_row

def transform_escola_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ESC_ATIVO'] = int(transformed_row.get('ESC_ATIVO'))
    transformed_row['ESC_BAIRRO'] = str(transformed_row.get('ESC_BAIRRO'))
    transformed_row['ESC_CEP'] = str(transformed_row.get('ESC_CEP'))
    transformed_row['ESC_CIDADE'] = str(transformed_row.get('ESC_CIDADE'))
    transformed_row['ESC_COMPLEMENTO'] = str(transformed_row.get('ESC_COMPLEMENTO'))
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
    transformed_row['ESC_ENDERECO'] = str(transformed_row.get('ESC_ENDERECO'))
    transformed_row['ESC_ID'] = int(transformed_row.get('ESC_ID'))
    transformed_row['ESC_INEP'] = str(transformed_row.get('ESC_INEP'))
    transformed_row['ESC_INTEGRAL'] = int(transformed_row.get('ESC_INTEGRAL'))
    transformed_row['ESC_LOGO'] = str(transformed_row.get('ESC_LOGO'))
    transformed_row['ESC_MUN_ID'] = int(transformed_row.get('ESC_MUN_ID'))
    transformed_row['ESC_NOME'] = str(transformed_row.get('ESC_NOME'))
    transformed_row['ESC_NUMERO'] = str(transformed_row.get('ESC_NUMERO'))
    transformed_row['ESC_OLD_ID'] = int(transformed_row.get('ESC_OLD_ID'))
    transformed_row['ESC_STATUS'] = str(transformed_row.get('ESC_STATUS'))
    transformed_row['ESC_TIPO'] = str(transformed_row.get('ESC_TIPO'))
    transformed_row['ESC_UF'] = str(transformed_row.get('ESC_UF'))
    transformed_row['regionalId'] = int(transformed_row.get('regionalId'))

    return transformed_row

def transform_estados_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['abbreviation'] = str(transformed_row.get('abbreviation'))
    transformed_row['active'] = int(transformed_row.get('active'))
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['name'] = str(transformed_row.get('name'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
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
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['isValid'] = int(transformed_row.get('isValid'))
    transformed_row['token'] = str(transformed_row.get('token'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['userUSUID'] = int(transformed_row.get('userUSUID'))

    return transformed_row

def transform_formacao_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['FOR_ATIVO'] = int(transformed_row.get('FOR_ATIVO'))
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
    transformed_row['FOR_ID'] = int(transformed_row.get('FOR_ID'))
    transformed_row['FOR_NOME'] = str(transformed_row.get('FOR_NOME'))

    return transformed_row

def transform_genero_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['GEN_ATIVO'] = int(transformed_row.get('GEN_ATIVO'))
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
    transformed_row['GEN_ID'] = int(transformed_row.get('GEN_ID'))
    transformed_row['GEN_NOME'] = str(transformed_row.get('GEN_NOME'))

    return transformed_row

def transform_infrequencia_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['IFR_ALU_ID'] = int(transformed_row.get('IFR_ALU_ID'))
    transformed_row['IFR_ANO'] = int(transformed_row.get('IFR_ANO'))
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
    transformed_row['IFR_FALTA'] = int(transformed_row.get('IFR_FALTA'))
    transformed_row['IFR_ID'] = int(transformed_row.get('IFR_ID'))
    transformed_row['IFR_MES'] = int(transformed_row.get('IFR_MES'))
    transformed_row['IFR_OLD_ID'] = int(transformed_row.get('IFR_OLD_ID'))
    transformed_row['IFR_SCHOOL_CLASS_ID'] = int(transformed_row.get('IFR_SCHOOL_CLASS_ID'))

    return transformed_row

def transform_job_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['assessmentId'] = int(transformed_row.get('assessmentId'))
    transformed_row['bullId'] = str(transformed_row.get('bullId'))
    transformed_row['countyId'] = int(transformed_row.get('countyId'))
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
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['jobType'] = str(transformed_row.get('jobType'))
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
        
    transformed_row['MAR_ATIVO'] = int(transformed_row.get('MAR_ATIVO'))
    transformed_row['MAR_DIS_ID'] = int(transformed_row.get('MAR_DIS_ID'))
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
    transformed_row['MAR_ID'] = int(transformed_row.get('MAR_ID'))
    transformed_row['MAR_NOME'] = str(transformed_row.get('MAR_NOME'))
    transformed_row['MAR_OLD_ID'] = int(transformed_row.get('MAR_OLD_ID'))

    return transformed_row

def transform_matriz_referencia_serie_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['MAR_ID'] = int(transformed_row.get('MAR_ID'))
    transformed_row['SER_ID'] = int(transformed_row.get('SER_ID'))

    return transformed_row

def transform_matriz_referencia_topico_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['MTO_ATIVO'] = int(transformed_row.get('MTO_ATIVO'))
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
    transformed_row['MTO_ID'] = int(transformed_row.get('MTO_ID'))
    transformed_row['MTO_MAR_ID'] = int(transformed_row.get('MTO_MAR_ID'))
    transformed_row['MTO_NOME'] = str(transformed_row.get('MTO_NOME'))
    transformed_row['MTO_OLD_ID'] = int(transformed_row.get('MTO_OLD_ID'))

    return transformed_row

def transform_matriz_referencia_topico_items_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['MTI_ATIVO'] = int(transformed_row.get('MTI_ATIVO'))
    transformed_row['MTI_CODIGO'] = str(transformed_row.get('MTI_CODIGO'))
    transformed_row['MTI_DESCRITOR'] = str(transformed_row.get('MTI_DESCRITOR'))
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
    transformed_row['MTI_ID'] = int(transformed_row.get('MTI_ID'))
    transformed_row['MTI_MTO_ID'] = int(transformed_row.get('MTI_MTO_ID'))
    transformed_row['MTI_OLD_ID'] = int(transformed_row.get('MTI_OLD_ID'))

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
    transformed_row['MEN_ID'] = int(transformed_row.get('MEN_ID'))
    transformed_row['MEN_IS_DELETE'] = int(transformed_row.get('MEN_IS_DELETE'))
    transformed_row['MEN_TEXT'] = str(transformed_row.get('MEN_TEXT'))
    transformed_row['MEN_TITLE'] = str(transformed_row.get('MEN_TITLE'))

    return transformed_row

def transform_messages_municipios_municipio_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['messagesMENID'] = int(transformed_row.get('messagesMENID'))
    transformed_row['municipioMUNID'] = int(transformed_row.get('municipioMUNID'))

    return transformed_row

def transform_messages_schools_escola_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['escolaESCID'] = int(transformed_row.get('escolaESCID'))
    transformed_row['messagesMENID'] = int(transformed_row.get('messagesMENID'))

    return transformed_row

def transform_migrations_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['name'] = str(transformed_row.get('name'))
    transformed_row['timestamp'] = int(transformed_row.get('timestamp'))

    return transformed_row

def transform_municipio_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['MUN_ARQ_CONVENIO'] = str(transformed_row.get('MUN_ARQ_CONVENIO'))
    transformed_row['MUN_ATIVO'] = int(transformed_row.get('MUN_ATIVO'))
    transformed_row['MUN_BAIRRO'] = str(transformed_row.get('MUN_BAIRRO'))
    transformed_row['MUN_CEP'] = str(transformed_row.get('MUN_CEP'))
    transformed_row['MUN_CIDADE'] = str(transformed_row.get('MUN_CIDADE'))
    transformed_row['MUN_COD_IBGE'] = int(transformed_row.get('MUN_COD_IBGE'))
    transformed_row['MUN_COMPARTILHAR_DADOS'] = int(transformed_row.get('MUN_COMPARTILHAR_DADOS'))
    transformed_row['MUN_COMPLEMENTO'] = str(transformed_row.get('MUN_COMPLEMENTO'))
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
    transformed_row['MUN_ENDERECO'] = str(transformed_row.get('MUN_ENDERECO'))
    transformed_row['MUN_ID'] = int(transformed_row.get('MUN_ID'))
    transformed_row['MUN_LOGO'] = str(transformed_row.get('MUN_LOGO'))
    transformed_row['MUN_MENSAGEM_EMAIL_ATIVO'] = int(transformed_row.get('MUN_MENSAGEM_EMAIL_ATIVO'))
    transformed_row['MUN_MENSAGEM_WHATSAPP_ATIVO'] = int(transformed_row.get('MUN_MENSAGEM_WHATSAPP_ATIVO'))
    transformed_row['MUN_NOME'] = str(transformed_row.get('MUN_NOME'))
    transformed_row['MUN_NUMERO'] = str(transformed_row.get('MUN_NUMERO'))
    transformed_row['MUN_OLD_ID'] = int(transformed_row.get('MUN_OLD_ID'))
    transformed_row['MUN_PARCEIRO_EPV'] = int(transformed_row.get('MUN_PARCEIRO_EPV'))
    transformed_row['MUN_STATUS'] = str(transformed_row.get('MUN_STATUS'))
    transformed_row['MUN_UF'] = str(transformed_row.get('MUN_UF'))
    transformed_row['stateId'] = int(transformed_row.get('stateId'))
    transformed_row['stateRegionalId'] = int(transformed_row.get('stateRegionalId'))

    return transformed_row

def transform_pcd_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['PCD_ATIVO'] = int(transformed_row.get('PCD_ATIVO'))
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
    transformed_row['PCD_ID'] = int(transformed_row.get('PCD_ID'))
    transformed_row['PCD_NOME'] = str(transformed_row.get('PCD_NOME'))
    transformed_row['PCD_OLD_ID'] = int(transformed_row.get('PCD_OLD_ID'))

    return transformed_row

def transform_perfil_base_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['PER_ATIVO'] = int(transformed_row.get('PER_ATIVO'))
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
    transformed_row['PER_ID'] = int(transformed_row.get('PER_ID'))
    transformed_row['PER_NOME'] = str(transformed_row.get('PER_NOME'))

    return transformed_row

def transform_professor_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['PRO_ATIVO'] = int(transformed_row.get('PRO_ATIVO'))
    transformed_row['PRO_AVATAR'] = str(transformed_row.get('PRO_AVATAR'))
    transformed_row['PRO_BAIRRO'] = str(transformed_row.get('PRO_BAIRRO'))
    transformed_row['PRO_CEP'] = str(transformed_row.get('PRO_CEP'))
    transformed_row['PRO_CIDADE'] = str(transformed_row.get('PRO_CIDADE'))
    transformed_row['PRO_COMPLEMENTO'] = str(transformed_row.get('PRO_COMPLEMENTO'))
    transformed_row['PRO_DOCUMENTO'] = str(transformed_row.get('PRO_DOCUMENTO'))
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
    transformed_row['PRO_EMAIL'] = str(transformed_row.get('PRO_EMAIL'))
    transformed_row['PRO_ENDERECO'] = str(transformed_row.get('PRO_ENDERECO'))
    transformed_row['PRO_FONE'] = str(transformed_row.get('PRO_FONE'))
    transformed_row['PRO_FOR_ID'] = int(transformed_row.get('PRO_FOR_ID'))
    transformed_row['PRO_GEN_ID'] = int(transformed_row.get('PRO_GEN_ID'))
    transformed_row['PRO_ID'] = int(transformed_row.get('PRO_ID'))
    transformed_row['PRO_MUN_ID'] = int(transformed_row.get('PRO_MUN_ID'))
    transformed_row['PRO_NOME'] = str(transformed_row.get('PRO_NOME'))
    transformed_row['PRO_NUMERO'] = str(transformed_row.get('PRO_NUMERO'))
    transformed_row['PRO_OLD_ID'] = int(transformed_row.get('PRO_OLD_ID'))
    transformed_row['PRO_PEL_ID'] = int(transformed_row.get('PRO_PEL_ID'))
    transformed_row['PRO_UF'] = str(transformed_row.get('PRO_UF'))

    return transformed_row

def transform_raca_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['PEL_ATIVO'] = int(transformed_row.get('PEL_ATIVO'))
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
    transformed_row['PEL_ID'] = int(transformed_row.get('PEL_ID'))
    transformed_row['PEL_NOME'] = str(transformed_row.get('PEL_NOME'))
    transformed_row['PEL_OLD_ID'] = int(transformed_row.get('PEL_OLD_ID'))

    return transformed_row

def transform_regionais_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['active'] = int(transformed_row.get('active'))
    transformed_row['countyId'] = int(transformed_row.get('countyId'))
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['name'] = str(transformed_row.get('name'))
    transformed_row['stateId'] = int(transformed_row.get('stateId'))
    transformed_row['type'] = str(transformed_row.get('type'))
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
    transformed_row['descriptorMTIID'] = int(transformed_row.get('descriptorMTIID'))
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['reportEditionId'] = int(transformed_row.get('reportEditionId'))
    transformed_row['testTESID'] = int(transformed_row.get('testTESID'))
    transformed_row['total'] = int(transformed_row.get('total'))
    transformed_row['totalCorrect'] = int(transformed_row.get('totalCorrect'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_edition_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['countyMUNID'] = int(transformed_row.get('countyMUNID'))
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['editionAVAID'] = int(transformed_row.get('editionAVAID'))
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['regionalId'] = int(transformed_row.get('regionalId'))
    transformed_row['schoolClassTURID'] = int(transformed_row.get('schoolClassTURID'))
    transformed_row['schoolESCID'] = int(transformed_row.get('schoolESCID'))
    transformed_row['type'] = str(transformed_row.get('type'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_not_evaluated_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['abandono'] = int(transformed_row.get('abandono'))
    transformed_row['ausencia'] = int(transformed_row.get('ausencia'))
    transformed_row['countPresentStudents'] = int(transformed_row.get('countPresentStudents'))
    transformed_row['countStudentsLaunched'] = int(transformed_row.get('countStudentsLaunched'))
    transformed_row['countTotalStudents'] = int(transformed_row.get('countTotalStudents'))
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['deficiencia'] = int(transformed_row.get('deficiencia'))
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['idStudents'] = str(transformed_row.get('idStudents'))
    transformed_row['name'] = str(transformed_row.get('name'))
    transformed_row['nao_participou'] = int(transformed_row.get('nao_participou'))
    transformed_row['recusa'] = int(transformed_row.get('recusa'))
    transformed_row['reportEditionId'] = int(transformed_row.get('reportEditionId'))
    transformed_row['testTESID'] = int(transformed_row.get('testTESID'))
    transformed_row['transferencia'] = int(transformed_row.get('transferencia'))
    transformed_row['type'] = str(transformed_row.get('type'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_question_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['fluente'] = int(transformed_row.get('fluente'))
    transformed_row['frases'] = int(transformed_row.get('frases'))
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['nao_avaliado'] = int(transformed_row.get('nao_avaliado'))
    transformed_row['nao_fluente'] = int(transformed_row.get('nao_fluente'))
    transformed_row['nao_informado'] = int(transformed_row.get('nao_informado'))
    transformed_row['nao_leitor'] = int(transformed_row.get('nao_leitor'))
    transformed_row['option_correct'] = str(transformed_row.get('option_correct'))
    transformed_row['palavras'] = int(transformed_row.get('palavras'))
    transformed_row['questionTEGID'] = int(transformed_row.get('questionTEGID'))
    transformed_row['reportSubjectId'] = int(transformed_row.get('reportSubjectId'))
    transformed_row['silabas'] = int(transformed_row.get('silabas'))
    transformed_row['total_a'] = int(transformed_row.get('total_a'))
    transformed_row['total_b'] = int(transformed_row.get('total_b'))
    transformed_row['total_c'] = int(transformed_row.get('total_c'))
    transformed_row['total_d'] = int(transformed_row.get('total_d'))
    transformed_row['total_null'] = int(transformed_row.get('total_null'))

    return transformed_row

def transform_report_race_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['countPresentStudents'] = int(transformed_row.get('countPresentStudents'))
    transformed_row['countStudentsLaunched'] = int(transformed_row.get('countStudentsLaunched'))
    transformed_row['countTotalStudents'] = int(transformed_row.get('countTotalStudents'))
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['fluente'] = int(transformed_row.get('fluente'))
    transformed_row['frases'] = int(transformed_row.get('frases'))
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['idStudents'] = str(transformed_row.get('idStudents'))
    transformed_row['name'] = str(transformed_row.get('name'))
    transformed_row['nao_avaliado'] = int(transformed_row.get('nao_avaliado'))
    transformed_row['nao_fluente'] = int(transformed_row.get('nao_fluente'))
    transformed_row['nao_informado'] = int(transformed_row.get('nao_informado'))
    transformed_row['nao_leitor'] = int(transformed_row.get('nao_leitor'))
    transformed_row['palavras'] = int(transformed_row.get('palavras'))
    transformed_row['racePELID'] = int(transformed_row.get('racePELID'))
    transformed_row['reportSubjectId'] = int(transformed_row.get('reportSubjectId'))
    transformed_row['silabas'] = int(transformed_row.get('silabas'))
    transformed_row['totalGradesStudents'] = int(transformed_row.get('totalGradesStudents'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_report_subject_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['countPresentStudents'] = int(transformed_row.get('countPresentStudents'))
    transformed_row['countStudentsLaunched'] = int(transformed_row.get('countStudentsLaunched'))
    transformed_row['countTotalStudents'] = int(transformed_row.get('countTotalStudents'))
    transformed_row['createdAt'] = formatar_data(
        valor_data=transformed_row.get('createdAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['fluente'] = int(transformed_row.get('fluente'))
    transformed_row['frases'] = int(transformed_row.get('frases'))
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['idStudents'] = str(transformed_row.get('idStudents'))
    transformed_row['name'] = str(transformed_row.get('name'))
    transformed_row['nao_avaliado'] = int(transformed_row.get('nao_avaliado'))
    transformed_row['nao_fluente'] = int(transformed_row.get('nao_fluente'))
    transformed_row['nao_informado'] = int(transformed_row.get('nao_informado'))
    transformed_row['nao_leitor'] = int(transformed_row.get('nao_leitor'))
    transformed_row['palavras'] = int(transformed_row.get('palavras'))
    transformed_row['reportEditionId'] = int(transformed_row.get('reportEditionId'))
    transformed_row['silabas'] = int(transformed_row.get('silabas'))
    transformed_row['testTESID'] = int(transformed_row.get('testTESID'))
    transformed_row['totalGradesStudents'] = int(transformed_row.get('totalGradesStudents'))
    transformed_row['type'] = str(transformed_row.get('type'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_series_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['SER_ATIVO'] = int(transformed_row.get('SER_ATIVO'))
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
    transformed_row['SER_ID'] = int(transformed_row.get('SER_ID'))
    transformed_row['SER_NOME'] = str(transformed_row.get('SER_NOME'))
    transformed_row['SER_NUMBER'] = int(transformed_row.get('SER_NUMBER'))
    transformed_row['SER_OLD_ID'] = int(transformed_row.get('SER_OLD_ID'))

    return transformed_row

def transform_sub_perfil_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['SPE_ATIVO'] = int(transformed_row.get('SPE_ATIVO'))
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
    transformed_row['SPE_ID'] = int(transformed_row.get('SPE_ID'))
    transformed_row['SPE_NOME'] = str(transformed_row.get('SPE_NOME'))
    transformed_row['SPE_PER_ID'] = int(transformed_row.get('SPE_PER_ID'))
    transformed_row['role'] = str(transformed_row.get('role'))

    return transformed_row

def transform_sub_perfil_area_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['ARE_ID'] = int(transformed_row.get('ARE_ID'))
    transformed_row['SPE_ID'] = int(transformed_row.get('SPE_ID'))

    return transformed_row

def transform_teste_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['TES_ANO'] = str(transformed_row.get('TES_ANO'))
    transformed_row['TES_ARQUIVO'] = str(transformed_row.get('TES_ARQUIVO'))
    transformed_row['TES_ATIVO'] = int(transformed_row.get('TES_ATIVO'))
    transformed_row['TES_DIS_ID'] = int(transformed_row.get('TES_DIS_ID'))
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
    transformed_row['TES_ID'] = int(transformed_row.get('TES_ID'))
    transformed_row['TES_MANUAL'] = str(transformed_row.get('TES_MANUAL'))
    transformed_row['TES_MAR_ID'] = int(transformed_row.get('TES_MAR_ID'))
    transformed_row['TES_NOME'] = str(transformed_row.get('TES_NOME'))
    transformed_row['TES_OLD_ID'] = int(transformed_row.get('TES_OLD_ID'))
    transformed_row['TES_SER_ID'] = int(transformed_row.get('TES_SER_ID'))
    transformed_row['assessmentOnlineId'] = int(transformed_row.get('assessmentOnlineId'))

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
    transformed_row['TEG_ID'] = int(transformed_row.get('TEG_ID'))
    transformed_row['TEG_MTI_ID'] = int(transformed_row.get('TEG_MTI_ID'))
    transformed_row['TEG_OLD_ID'] = int(transformed_row.get('TEG_OLD_ID'))
    transformed_row['TEG_ORDEM'] = int(transformed_row.get('TEG_ORDEM'))
    transformed_row['TEG_RESPOSTA_CORRETA'] = str(transformed_row.get('TEG_RESPOSTA_CORRETA'))
    transformed_row['TEG_TES_ID'] = int(transformed_row.get('TEG_TES_ID'))

    return transformed_row

def transform_transferencia_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['TRF_ALU_ID'] = int(transformed_row.get('TRF_ALU_ID'))
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
    transformed_row['TRF_ESC_ID_DESTINO'] = int(transformed_row.get('TRF_ESC_ID_DESTINO'))
    transformed_row['TRF_ESC_ID_ORIGEM'] = int(transformed_row.get('TRF_ESC_ID_ORIGEM'))
    transformed_row['TRF_ID'] = int(transformed_row.get('TRF_ID'))
    transformed_row['TRF_JUSTIFICATIVA'] = str(transformed_row.get('TRF_JUSTIFICATIVA'))
    transformed_row['TRF_OLD_ID'] = int(transformed_row.get('TRF_OLD_ID'))
    transformed_row['TRF_STATUS'] = str(transformed_row.get('TRF_STATUS'))
    transformed_row['TRF_TUR_ID_DESTINO'] = int(transformed_row.get('TRF_TUR_ID_DESTINO'))
    transformed_row['TRF_TUR_ID_ORIGEM'] = int(transformed_row.get('TRF_TUR_ID_ORIGEM'))
    transformed_row['TRF_USU'] = int(transformed_row.get('TRF_USU'))
    transformed_row['TRF_USU_STATUS'] = int(transformed_row.get('TRF_USU_STATUS'))

    return transformed_row

def transform_turma_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['TUR_ANEXO'] = int(transformed_row.get('TUR_ANEXO'))
    transformed_row['TUR_ANO'] = str(transformed_row.get('TUR_ANO'))
    transformed_row['TUR_ATIVO'] = int(transformed_row.get('TUR_ATIVO'))
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
    transformed_row['TUR_ESC_ID'] = int(transformed_row.get('TUR_ESC_ID'))
    transformed_row['TUR_ID'] = int(transformed_row.get('TUR_ID'))
    transformed_row['TUR_MUN_ID'] = int(transformed_row.get('TUR_MUN_ID'))
    transformed_row['TUR_NOME'] = str(transformed_row.get('TUR_NOME'))
    transformed_row['TUR_OLD_ID'] = int(transformed_row.get('TUR_OLD_ID'))
    transformed_row['TUR_PERIODO'] = str(transformed_row.get('TUR_PERIODO'))
    transformed_row['TUR_SER_ID'] = int(transformed_row.get('TUR_SER_ID'))
    transformed_row['TUR_TIPO'] = str(transformed_row.get('TUR_TIPO'))

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
    transformed_row['id'] = int(transformed_row.get('id'))
    transformed_row['schoolClassTURID'] = int(transformed_row.get('schoolClassTURID'))
    transformed_row['startDate'] = formatar_data(
        valor_data=transformed_row.get('startDate'),
        formato_entrada='%Y-%m-%d',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )
    transformed_row['studentALUID'] = int(transformed_row.get('studentALUID'))
    transformed_row['updatedAt'] = formatar_data(
        valor_data=transformed_row.get('updatedAt'),
        formato_entrada='%Y-%m-%d %H:%M:%S.%f',
        formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
    )

    return transformed_row

def transform_turma_professor_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['PRO_ID'] = int(transformed_row.get('PRO_ID'))
    transformed_row['TUR_ID'] = int(transformed_row.get('TUR_ID'))

    return transformed_row

def transform_usuario_table(row_dict):
    transformed_row = row_dict.copy()
        
    transformed_row['USU_ATIVO'] = int(transformed_row.get('USU_ATIVO'))
    transformed_row['USU_AVATAR'] = str(transformed_row.get('USU_AVATAR'))
    transformed_row['USU_DOCUMENTO'] = str(transformed_row.get('USU_DOCUMENTO'))
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
    transformed_row['USU_EMAIL'] = str(transformed_row.get('USU_EMAIL'))
    transformed_row['USU_ESC_ID'] = int(transformed_row.get('USU_ESC_ID'))
    transformed_row['USU_FONE'] = str(transformed_row.get('USU_FONE'))
    transformed_row['USU_ID'] = int(transformed_row.get('USU_ID'))
    transformed_row['USU_MUN_ID'] = int(transformed_row.get('USU_MUN_ID'))
    transformed_row['USU_NOME'] = str(transformed_row.get('USU_NOME'))
    transformed_row['USU_SENHA'] = str(transformed_row.get('USU_SENHA'))
    transformed_row['USU_SPE_ID'] = int(transformed_row.get('USU_SPE_ID'))
    transformed_row['isChangePasswordWelcome'] = int(transformed_row.get('isChangePasswordWelcome'))
    transformed_row['stateId'] = int(transformed_row.get('stateId'))

    return transformed_row

TRANSFORM_MAPPING = {
    'aluno': transform_aluno_table,
    'aluno_alu_deficiencias_pcd': transform_aluno_alu_deficiencias_pcd_table,
    'aluno_teste': transform_aluno_teste_table,
    'aluno_teste_resposta': transform_aluno_teste_resposta_table,
    'aluno_teste_resposta_historico': transform_aluno_teste_resposta_historico_table,
    'ano_letivo': transform_ano_letivo_table,
    'area': transform_area_table,
    'avaliacao': transform_avaliacao_table,
    'avaliacao_municipio': transform_avaliacao_municipio_table,
    'avaliacao_online': transform_avaliacao_online_table,
    'avaliacao_online_page': transform_avaliacao_online_page_table,
    'avaliacao_online_question': transform_avaliacao_online_question_table,
    'avaliacao_online_question_alternative': transform_avaliacao_online_question_alternative_table,
    'avaliacao_teste': transform_avaliacao_teste_table,
    'disciplina': transform_disciplina_table,
    'escola': transform_escola_table,
    'estados': transform_estados_table,
    'forget_password': transform_forget_password_table,
    'formacao': transform_formacao_table,
    'genero': transform_genero_table,
    'infrequencia': transform_infrequencia_table,
    'job': transform_job_table,
    'matriz_referencia': transform_matriz_referencia_table,
    'matriz_referencia_serie': transform_matriz_referencia_serie_table,
    'matriz_referencia_topico': transform_matriz_referencia_topico_table,
    'matriz_referencia_topico_items': transform_matriz_referencia_topico_items_table,
    'messages': transform_messages_table,
    'messages_municipios_municipio': transform_messages_municipios_municipio_table,
    'messages_schools_escola': transform_messages_schools_escola_table,
    'migrations': transform_migrations_table,
    'municipio': transform_municipio_table,
    'pcd': transform_pcd_table,
    'perfil_base': transform_perfil_base_table,
    'professor': transform_professor_table,
    'raca': transform_raca_table,
    'regionais': transform_regionais_table,
    'report_descriptor': transform_report_descriptor_table,
    'report_edition': transform_report_edition_table,
    'report_not_evaluated': transform_report_not_evaluated_table,
    'report_question': transform_report_question_table,
    'report_race': transform_report_race_table,
    'report_subject': transform_report_subject_table,
    'series': transform_series_table,
    'sub_perfil': transform_sub_perfil_table,
    'sub_perfil_area': transform_sub_perfil_area_table,
    'teste': transform_teste_table,
    'teste_gabarito': transform_teste_gabarito_table,
    'transferencia': transform_transferencia_table,
    'turma': transform_turma_table,
    'turma_aluno': transform_turma_aluno_table,
    'turma_professor': transform_turma_professor_table,
    'usuario': transform_usuario_table,
    }
