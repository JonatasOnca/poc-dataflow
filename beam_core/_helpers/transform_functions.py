# Copyright 2025 TecOnca Data Solutions.


import logging

from decimal import Decimal
from datetime import datetime, timedelta

def converter_data(valor_data, formato_entrada, formato_saida='%Y-%m-%d %H:%M:%S.%f'):
    # Formato de destino: Ano-Mês-Dia Hora:Minuto:Segundo.Microssegundo
    try:
        # Tenta a conversão para string de data/tempo
        data_convertida = None
        
        # 1. Tenta converter de número serial do Excel (OLE Automation Date)
        # O número serial é uma string que pode conter vírgula para separar a parte fracionária
        if isinstance(valor_data, str):
            # Substitui a vírgula por ponto para garantir a correta conversão para float
            valor_data_formatado = valor_data.replace(',', '.')
            if valor_data_formatado.replace('.', '', 1).isdigit(): # Verifica se é um número (float ou int)
                data_serial = float(valor_data_formatado)
                
                # OLE Automation Date começa em 30 de dezembro de 1899
                # 25569 é o número serial para 1 de janeiro de 1970 (para referência)
                
                # Base de data do Excel (30 de dezembro de 1899)
                data_base_excel = datetime(1899, 12, 30)
                
                # Converte a parte serial em um objeto timedelta
                # A parte inteira é o número de dias, a parte fracionária é a fração de um dia
                dias = int(data_serial)
                fracao_tempo_dias = data_serial - dias
                
                # O número de segundos é a fração do dia * (24 * 60 * 60)
                segundos_do_dia = fracao_tempo_dias * 86400  # 86400 segundos em um dia
                
                # Cria o timedelta
                delta = timedelta(days=dias, seconds=segundos_do_dia)
                
                data_convertida = data_base_excel + delta
                
        # 2. Tenta converter a partir de string de data usando diversos formatos
        if data_convertida is None:
            # Lista de formatos possíveis baseada na lista fornecida
            formatos_string = [
                '%Y-%m-%d %H:%M:%S.%f %Z',  # Ex: 2025-10-07 20:04:19.596331
                '%Y-%m-%d %H:%M:%S %Z',    # Ex: 2024-12-28 13:46:29
                '%Y-%m-%dT%H:%M:%S.%f',    # Ex: 2025-10-08 00:52:14.112527 (ISO-like sem fuso)
                '%Y-%m-%d %H:%M:%S',       # Ex: 47068 (aqui seria um serial, mas para ser exaustivo)
                '%Y-%m-%d',                # Ex: 45938
                # Nota: Os formatos com %Z (timezone) podem falhar se a string não tiver o fuso horário.
                # Nesses casos, a conversão é tentada sem o %Z para obter um objeto datetime naive.
            ]
            
            # Tenta converter a string com e sem %Z
            for fmt in list(set(formatos_string)): # Usamos set para evitar duplicações
                try:
                    # Tenta a conversão
                    data_convertida = datetime.strptime(valor_data, fmt)
                    break
                except ValueError:
                    # Se falhar, tenta remover %Z, se presente
                    if '%Z' in fmt:
                        try:
                            fmt_sem_tz = fmt.replace(' %Z', '')
                            data_convertida = datetime.strptime(valor_data, fmt_sem_tz)
                            break
                        except ValueError:
                            continue # Passa para o próximo formato

            
        # 3. Formata a data convertida para a string de formato padrão
        if data_convertida:
            # Retorna a nova tupla com a data formatada
            return data_convertida.strftime(formato_saida)

        # Se a conversão for um número inteiro que não é um serial Excel, pode ser um dia.
        # Mas sem informação mais precisa, manteremos a lógica acima como prioritária.
        
        # Se nenhuma conversão funcionar, retorna o valor original.
        return valor_data

    except Exception as e:
        # Em caso de qualquer erro inesperado, retorna a tupla original
        logging.warning(f"ERRO ao analisar: {e} | Valor original: '{valor_data}' com formato '{formato_entrada}'")
        return valor_data


def generic_transform(row_dict):
    return row_dict

def transform_aluno_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('ALU_ATIVO') is None:
        transformed_row['ALU_ATIVO'] = int(transformed_row.get('ALU_ATIVO'))
    if not transformed_row.get('ALU_AVATAR') is None:
        transformed_row['ALU_AVATAR'] = str(transformed_row.get('ALU_AVATAR'))
    if not transformed_row.get('ALU_BAIRRO') is None:
        transformed_row['ALU_BAIRRO'] = str(transformed_row.get('ALU_BAIRRO'))
    if not transformed_row.get('ALU_CEP') is None:
        transformed_row['ALU_CEP'] = str(transformed_row.get('ALU_CEP'))
    if not transformed_row.get('ALU_CIDADE') is None:
        transformed_row['ALU_CIDADE'] = str(transformed_row.get('ALU_CIDADE'))
    if not transformed_row.get('ALU_COD') is None:
        transformed_row['ALU_COD'] = int(transformed_row.get('ALU_COD'))
    if not transformed_row.get('ALU_COMPLEMENTO') is None:
        transformed_row['ALU_COMPLEMENTO'] = str(transformed_row.get('ALU_COMPLEMENTO'))
    if not transformed_row.get('ALU_CPF') is None:
        transformed_row['ALU_CPF'] = str(transformed_row.get('ALU_CPF'))
    if not transformed_row.get('ALU_DEFICIENCIA_BY_IMPORT') is None:
        transformed_row['ALU_DEFICIENCIA_BY_IMPORT'] = str(transformed_row.get('ALU_DEFICIENCIA_BY_IMPORT'))
    if not transformed_row.get('ALU_DT_ATUALIZACAO') is None:
        transformed_row['ALU_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('ALU_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ALU_DT_CRIACAO') is None:
        transformed_row['ALU_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('ALU_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ALU_DT_NASC') is None:
        transformed_row['ALU_DT_NASC'] = str(transformed_row.get('ALU_DT_NASC'))
    if not transformed_row.get('ALU_EMAIL') is None:
        transformed_row['ALU_EMAIL'] = str(transformed_row.get('ALU_EMAIL'))
    if not transformed_row.get('ALU_ENDERECO') is None:
        transformed_row['ALU_ENDERECO'] = str(transformed_row.get('ALU_ENDERECO'))
    if not transformed_row.get('ALU_ESC_ID') is None:
        transformed_row['ALU_ESC_ID'] = int(transformed_row.get('ALU_ESC_ID'))
    if not transformed_row.get('ALU_GEN_ID') is None:
        transformed_row['ALU_GEN_ID'] = int(transformed_row.get('ALU_GEN_ID'))
    if not transformed_row.get('ALU_ID') is None:
        transformed_row['ALU_ID'] = int(transformed_row.get('ALU_ID'))
    if not transformed_row.get('ALU_INEP') is None:
        transformed_row['ALU_INEP'] = str(transformed_row.get('ALU_INEP'))
    if not transformed_row.get('ALU_NOME') is None:
        transformed_row['ALU_NOME'] = str(transformed_row.get('ALU_NOME'))
    if not transformed_row.get('ALU_NOME_MAE') is None:
        transformed_row['ALU_NOME_MAE'] = str(transformed_row.get('ALU_NOME_MAE'))
    if not transformed_row.get('ALU_NOME_PAI') is None:
        transformed_row['ALU_NOME_PAI'] = str(transformed_row.get('ALU_NOME_PAI'))
    if not transformed_row.get('ALU_NOME_RESP') is None:
        transformed_row['ALU_NOME_RESP'] = str(transformed_row.get('ALU_NOME_RESP'))
    if not transformed_row.get('ALU_NUMERO') is None:
        transformed_row['ALU_NUMERO'] = str(transformed_row.get('ALU_NUMERO'))
    if not transformed_row.get('ALU_OLD_ID') is None:
        transformed_row['ALU_OLD_ID'] = int(transformed_row.get('ALU_OLD_ID'))
    if not transformed_row.get('ALU_PCD_ID') is None:
        transformed_row['ALU_PCD_ID'] = int(transformed_row.get('ALU_PCD_ID'))
    if not transformed_row.get('ALU_PEL_ID') is None:
        transformed_row['ALU_PEL_ID'] = int(transformed_row.get('ALU_PEL_ID'))
    if not transformed_row.get('ALU_SER_ID') is None:
        transformed_row['ALU_SER_ID'] = int(transformed_row.get('ALU_SER_ID'))
    if not transformed_row.get('ALU_STATUS') is None:
        transformed_row['ALU_STATUS'] = str(transformed_row.get('ALU_STATUS'))
    if not transformed_row.get('ALU_TEL1') is None:
        transformed_row['ALU_TEL1'] = str(transformed_row.get('ALU_TEL1'))
    if not transformed_row.get('ALU_TEL2') is None:
        transformed_row['ALU_TEL2'] = str(transformed_row.get('ALU_TEL2'))
    if not transformed_row.get('ALU_TUR_ID') is None:
        transformed_row['ALU_TUR_ID'] = int(transformed_row.get('ALU_TUR_ID'))
    if not transformed_row.get('ALU_UF') is None:
        transformed_row['ALU_UF'] = str(transformed_row.get('ALU_UF'))
    if not transformed_row.get('ALU_WHATSAPP') is None:
        transformed_row['ALU_WHATSAPP'] = str(transformed_row.get('ALU_WHATSAPP'))

    return transformed_row

def transform_aluno_alu_deficiencias_pcd_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('alunoALUID') is None:
        transformed_row['alunoALUID'] = int(transformed_row.get('alunoALUID'))
    if not transformed_row.get('pcdPCDID') is None:
        transformed_row['pcdPCDID'] = int(transformed_row.get('pcdPCDID'))

    return transformed_row

def transform_aluno_teste_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('ALT_ALU_ID') is None:
        transformed_row['ALT_ALU_ID'] = int(transformed_row.get('ALT_ALU_ID'))
    if not transformed_row.get('ALT_ATIVO') is None:
        transformed_row['ALT_ATIVO'] = int(transformed_row.get('ALT_ATIVO'))
    if not transformed_row.get('ALT_BY_AVA_ONLINE') is None:
        transformed_row['ALT_BY_AVA_ONLINE'] = int(transformed_row.get('ALT_BY_AVA_ONLINE'))
    if not transformed_row.get('ALT_BY_EDLER') is None:
        transformed_row['ALT_BY_EDLER'] = int(transformed_row.get('ALT_BY_EDLER'))
    if not transformed_row.get('ALT_BY_HERBY') is None:
        transformed_row['ALT_BY_HERBY'] = int(transformed_row.get('ALT_BY_HERBY'))
    if not transformed_row.get('ALT_DT_ATUALIZACAO') is None:
        transformed_row['ALT_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('ALT_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ALT_DT_CRIACAO') is None:
        transformed_row['ALT_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('ALT_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ALT_FINALIZADO') is None:
        transformed_row['ALT_FINALIZADO'] = int(transformed_row.get('ALT_FINALIZADO'))
    if not transformed_row.get('ALT_FORNECEDOR') is None:
        transformed_row['ALT_FORNECEDOR'] = str(transformed_row.get('ALT_FORNECEDOR'))
    if not transformed_row.get('ALT_ID') is None:
        transformed_row['ALT_ID'] = int(transformed_row.get('ALT_ID'))
    if not transformed_row.get('ALT_JUSTIFICATIVA') is None:
        transformed_row['ALT_JUSTIFICATIVA'] = str(transformed_row.get('ALT_JUSTIFICATIVA'))
    if not transformed_row.get('ALT_TES_ID') is None:
        transformed_row['ALT_TES_ID'] = int(transformed_row.get('ALT_TES_ID'))
    if not transformed_row.get('ALT_USU_ID') is None:
        transformed_row['ALT_USU_ID'] = int(transformed_row.get('ALT_USU_ID'))
    if not transformed_row.get('schoolClassTURID') is None:
        transformed_row['schoolClassTURID'] = int(transformed_row.get('schoolClassTURID'))

    return transformed_row

def transform_aluno_teste_resposta_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('ATR_ALT_ID') is None:
        transformed_row['ATR_ALT_ID'] = int(transformed_row.get('ATR_ALT_ID'))
    if not transformed_row.get('ATR_CERTO') is None:
        transformed_row['ATR_CERTO'] = int(transformed_row.get('ATR_CERTO'))
    if not transformed_row.get('ATR_DT_CRIACAO') is None:
        transformed_row['ATR_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('ATR_DT_CRIACAO'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ATR_ID') is None:
        transformed_row['ATR_ID'] = int(transformed_row.get('ATR_ID'))
    if not transformed_row.get('ATR_MTI_ID') is None:
        transformed_row['ATR_MTI_ID'] = int(transformed_row.get('ATR_MTI_ID'))
    if not transformed_row.get('ATR_RESPOSTA') is None:
        transformed_row['ATR_RESPOSTA'] = str(transformed_row.get('ATR_RESPOSTA'))
    if not transformed_row.get('questionTemplateTEGID') is None:
        transformed_row['questionTemplateTEGID'] = int(transformed_row.get('questionTemplateTEGID'))

    return transformed_row

def transform_aluno_teste_resposta_historico_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('ATH_ALT_ID') is None:
        transformed_row['ATH_ALT_ID'] = int(transformed_row.get('ATH_ALT_ID'))
    if not transformed_row.get('ATH_ATR_ID') is None:
        transformed_row['ATH_ATR_ID'] = int(transformed_row.get('ATH_ATR_ID'))
    if not transformed_row.get('ATH_ATR_RESPOSTA_ANTIGA') is None:
        transformed_row['ATH_ATR_RESPOSTA_ANTIGA'] = str(transformed_row.get('ATH_ATR_RESPOSTA_ANTIGA'))
    if not transformed_row.get('ATH_ATR_RESPOSTA_NOVA') is None:
        transformed_row['ATH_ATR_RESPOSTA_NOVA'] = str(transformed_row.get('ATH_ATR_RESPOSTA_NOVA'))
    if not transformed_row.get('ATH_DT_CRIACAO') is None:
        transformed_row['ATH_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('ATH_DT_CRIACAO'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ATH_ID') is None:
        transformed_row['ATH_ID'] = int(transformed_row.get('ATH_ID'))
    if not transformed_row.get('ATH_OPERACAO') is None:
        transformed_row['ATH_OPERACAO'] = str(transformed_row.get('ATH_OPERACAO'))
    if not transformed_row.get('ATH_TEG_ID') is None:
        transformed_row['ATH_TEG_ID'] = int(transformed_row.get('ATH_TEG_ID'))

    return transformed_row

def transform_ano_letivo_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('ANO_ATIVO') is None:
        transformed_row['ANO_ATIVO'] = int(transformed_row.get('ANO_ATIVO'))
    if not transformed_row.get('ANO_DT_ATUALIZACAO') is None:
        transformed_row['ANO_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('ANO_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ANO_DT_CRIACAO') is None:
        transformed_row['ANO_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('ANO_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ANO_ID') is None:
        transformed_row['ANO_ID'] = int(transformed_row.get('ANO_ID'))
    if not transformed_row.get('ANO_NOME') is None:
        transformed_row['ANO_NOME'] = str(transformed_row.get('ANO_NOME'))
    if not transformed_row.get('ANO_OLD_ID') is None:
        transformed_row['ANO_OLD_ID'] = int(transformed_row.get('ANO_OLD_ID'))

    return transformed_row

def transform_area_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('ARE_ATIVO') is None:
        transformed_row['ARE_ATIVO'] = int(transformed_row.get('ARE_ATIVO'))
    if not transformed_row.get('ARE_DESCRICAO') is None:
        transformed_row['ARE_DESCRICAO'] = str(transformed_row.get('ARE_DESCRICAO'))
    if not transformed_row.get('ARE_DT_ATUALIZACAO') is None:
        transformed_row['ARE_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('ARE_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ARE_DT_CRIACAO') is None:
        transformed_row['ARE_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('ARE_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ARE_ID') is None:
        transformed_row['ARE_ID'] = int(transformed_row.get('ARE_ID'))
    if not transformed_row.get('ARE_NOME') is None:
        transformed_row['ARE_NOME'] = str(transformed_row.get('ARE_NOME'))

    return transformed_row

def transform_avaliacao_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('AVA_ANO') is None:
        transformed_row['AVA_ANO'] = str(transformed_row.get('AVA_ANO'))
    if not transformed_row.get('AVA_ATIVO') is None:
        transformed_row['AVA_ATIVO'] = int(transformed_row.get('AVA_ATIVO'))
    if not transformed_row.get('AVA_DT_ATUALIZACAO') is None:
        transformed_row['AVA_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('AVA_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('AVA_DT_CRIACAO') is None:
        transformed_row['AVA_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('AVA_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('AVA_ID') is None:
        transformed_row['AVA_ID'] = int(transformed_row.get('AVA_ID'))
    if not transformed_row.get('AVA_NOME') is None:
        transformed_row['AVA_NOME'] = str(transformed_row.get('AVA_NOME'))
    if not transformed_row.get('AVA_OLD_ID') is None:
        transformed_row['AVA_OLD_ID'] = int(transformed_row.get('AVA_OLD_ID'))

    return transformed_row

def transform_avaliacao_municipio_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('AVM_ATIVO') is None:
        transformed_row['AVM_ATIVO'] = int(transformed_row.get('AVM_ATIVO'))
    if not transformed_row.get('AVM_AVA_ID') is None:
        transformed_row['AVM_AVA_ID'] = int(transformed_row.get('AVM_AVA_ID'))
    if not transformed_row.get('AVM_DT_ATUALIZACAO') is None:
        transformed_row['AVM_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('AVM_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('AVM_DT_CRIACAO') is None:
        transformed_row['AVM_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('AVM_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('AVM_DT_DISPONIVEL') is None:
        transformed_row['AVM_DT_DISPONIVEL'] = converter_data(
            valor_data=transformed_row.get('AVM_DT_DISPONIVEL'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('AVM_DT_FIM') is None:
        transformed_row['AVM_DT_FIM'] = converter_data(
            valor_data=transformed_row.get('AVM_DT_FIM'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('AVM_DT_INICIO') is None:
        transformed_row['AVM_DT_INICIO'] = converter_data(
            valor_data=transformed_row.get('AVM_DT_INICIO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('AVM_ID') is None:
        transformed_row['AVM_ID'] = int(transformed_row.get('AVM_ID'))
    if not transformed_row.get('AVM_MUN_ID') is None:
        transformed_row['AVM_MUN_ID'] = int(transformed_row.get('AVM_MUN_ID'))
    if not transformed_row.get('AVM_OLD_ID') is None:
        transformed_row['AVM_OLD_ID'] = int(transformed_row.get('AVM_OLD_ID'))
    if not transformed_row.get('AVM_TIPO') is None:
        transformed_row['AVM_TIPO'] = str(transformed_row.get('AVM_TIPO'))

    return transformed_row

def transform_avaliacao_online_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('active') is None:
        transformed_row['active'] = int(transformed_row.get('active'))
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_avaliacao_online_page_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('assessmentOnlineId') is None:
        transformed_row['assessmentOnlineId'] = int(transformed_row.get('assessmentOnlineId'))
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('image') is None:
        transformed_row['image'] = str(transformed_row.get('image'))
    if not transformed_row.get('order') is None:
        transformed_row['order'] = int(transformed_row.get('order'))
    if not transformed_row.get('title') is None:
        transformed_row['title'] = str(transformed_row.get('title'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_avaliacao_online_question_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('description') is None:
        transformed_row['description'] = str(transformed_row.get('description'))
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('order') is None:
        transformed_row['order'] = int(transformed_row.get('order'))
    if not transformed_row.get('pageId') is None:
        transformed_row['pageId'] = int(transformed_row.get('pageId'))
    if not transformed_row.get('questionTemplateId') is None:
        transformed_row['questionTemplateId'] = int(transformed_row.get('questionTemplateId'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_avaliacao_online_question_alternative_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('description') is None:
        transformed_row['description'] = str(transformed_row.get('description'))
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('image') is None:
        transformed_row['image'] = str(transformed_row.get('image'))
    if not transformed_row.get('option') is None:
        transformed_row['option'] = str(transformed_row.get('option'))
    if not transformed_row.get('questionId') is None:
        transformed_row['questionId'] = int(transformed_row.get('questionId'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_avaliacao_teste_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('AVA_ID') is None:
        transformed_row['AVA_ID'] = int(transformed_row.get('AVA_ID'))
    if not transformed_row.get('TES_ID') is None:
        transformed_row['TES_ID'] = int(transformed_row.get('TES_ID'))

    return transformed_row

def transform_disciplina_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('DIS_ATIVO') is None:
        transformed_row['DIS_ATIVO'] = int(transformed_row.get('DIS_ATIVO'))
    if not transformed_row.get('DIS_COLOR') is None:
        transformed_row['DIS_COLOR'] = str(transformed_row.get('DIS_COLOR'))
    if not transformed_row.get('DIS_DT_ATUALIZACAO') is None:
        transformed_row['DIS_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('DIS_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('DIS_DT_CRIACAO') is None:
        transformed_row['DIS_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('DIS_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('DIS_ID') is None:
        transformed_row['DIS_ID'] = int(transformed_row.get('DIS_ID'))
    if not transformed_row.get('DIS_NOME') is None:
        transformed_row['DIS_NOME'] = str(transformed_row.get('DIS_NOME'))
    if not transformed_row.get('DIS_OLD_ID') is None:
        transformed_row['DIS_OLD_ID'] = int(transformed_row.get('DIS_OLD_ID'))
    if not transformed_row.get('DIS_TIPO') is None:
        transformed_row['DIS_TIPO'] = str(transformed_row.get('DIS_TIPO'))

    return transformed_row

def transform_escola_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('ESC_ATIVO') is None:
        transformed_row['ESC_ATIVO'] = int(transformed_row.get('ESC_ATIVO'))
    if not transformed_row.get('ESC_BAIRRO') is None:
        transformed_row['ESC_BAIRRO'] = str(transformed_row.get('ESC_BAIRRO'))
    if not transformed_row.get('ESC_CEP') is None:
        transformed_row['ESC_CEP'] = str(transformed_row.get('ESC_CEP'))
    if not transformed_row.get('ESC_CIDADE') is None:
        transformed_row['ESC_CIDADE'] = str(transformed_row.get('ESC_CIDADE'))
    if not transformed_row.get('ESC_COMPLEMENTO') is None:
        transformed_row['ESC_COMPLEMENTO'] = str(transformed_row.get('ESC_COMPLEMENTO'))
    if not transformed_row.get('ESC_DT_ATUALIZACAO') is None:
        transformed_row['ESC_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('ESC_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ESC_DT_CRIACAO') is None:
        transformed_row['ESC_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('ESC_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('ESC_ENDERECO') is None:
        transformed_row['ESC_ENDERECO'] = str(transformed_row.get('ESC_ENDERECO'))
    if not transformed_row.get('ESC_ID') is None:
        transformed_row['ESC_ID'] = int(transformed_row.get('ESC_ID'))
    if not transformed_row.get('ESC_INEP') is None:
        transformed_row['ESC_INEP'] = str(transformed_row.get('ESC_INEP'))
    if not transformed_row.get('ESC_INTEGRAL') is None:
        transformed_row['ESC_INTEGRAL'] = int(transformed_row.get('ESC_INTEGRAL'))
    if not transformed_row.get('ESC_LOGO') is None:
        transformed_row['ESC_LOGO'] = str(transformed_row.get('ESC_LOGO'))
    if not transformed_row.get('ESC_MUN_ID') is None:
        transformed_row['ESC_MUN_ID'] = int(transformed_row.get('ESC_MUN_ID'))
    if not transformed_row.get('ESC_NOME') is None:
        transformed_row['ESC_NOME'] = str(transformed_row.get('ESC_NOME'))
    if not transformed_row.get('ESC_NUMERO') is None:
        transformed_row['ESC_NUMERO'] = str(transformed_row.get('ESC_NUMERO'))
    if not transformed_row.get('ESC_OLD_ID') is None:
        transformed_row['ESC_OLD_ID'] = int(transformed_row.get('ESC_OLD_ID'))
    if not transformed_row.get('ESC_STATUS') is None:
        transformed_row['ESC_STATUS'] = str(transformed_row.get('ESC_STATUS'))
    if not transformed_row.get('ESC_TIPO') is None:
        transformed_row['ESC_TIPO'] = str(transformed_row.get('ESC_TIPO'))
    if not transformed_row.get('ESC_UF') is None:
        transformed_row['ESC_UF'] = str(transformed_row.get('ESC_UF'))
    if not transformed_row.get('regionalId') is None:
        transformed_row['regionalId'] = int(transformed_row.get('regionalId'))

    return transformed_row

def transform_estados_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('abbreviation') is None:
        transformed_row['abbreviation'] = str(transformed_row.get('abbreviation'))
    if not transformed_row.get('active') is None:
        transformed_row['active'] = int(transformed_row.get('active'))
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('name') is None:
        transformed_row['name'] = str(transformed_row.get('name'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_forget_password_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('isValid') is None:
        transformed_row['isValid'] = int(transformed_row.get('isValid'))
    if not transformed_row.get('token') is None:
        transformed_row['token'] = str(transformed_row.get('token'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('userUSUID') is None:
        transformed_row['userUSUID'] = int(transformed_row.get('userUSUID'))

    return transformed_row

def transform_formacao_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('FOR_ATIVO') is None:
        transformed_row['FOR_ATIVO'] = int(transformed_row.get('FOR_ATIVO'))
    if not transformed_row.get('FOR_DT_ATUALIZACAO') is None:
        transformed_row['FOR_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('FOR_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('FOR_DT_CRIACAO') is None:
        transformed_row['FOR_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('FOR_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('FOR_ID') is None:
        transformed_row['FOR_ID'] = int(transformed_row.get('FOR_ID'))
    if not transformed_row.get('FOR_NOME') is None:
        transformed_row['FOR_NOME'] = str(transformed_row.get('FOR_NOME'))

    return transformed_row

def transform_genero_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('GEN_ATIVO') is None:
        transformed_row['GEN_ATIVO'] = int(transformed_row.get('GEN_ATIVO'))
    if not transformed_row.get('GEN_DT_ATUALIZACAO') is None:
        transformed_row['GEN_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('GEN_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('GEN_DT_CRIACAO') is None:
        transformed_row['GEN_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('GEN_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('GEN_ID') is None:
        transformed_row['GEN_ID'] = int(transformed_row.get('GEN_ID'))
    if not transformed_row.get('GEN_NOME') is None:
        transformed_row['GEN_NOME'] = str(transformed_row.get('GEN_NOME'))

    return transformed_row

def transform_infrequencia_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('IFR_ALU_ID') is None:
        transformed_row['IFR_ALU_ID'] = int(transformed_row.get('IFR_ALU_ID'))
    if not transformed_row.get('IFR_ANO') is None:
        transformed_row['IFR_ANO'] = int(transformed_row.get('IFR_ANO'))
    if not transformed_row.get('IFR_DT_ATUALIZACAO') is None:
        transformed_row['IFR_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('IFR_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('IFR_DT_CRIACAO') is None:
        transformed_row['IFR_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('IFR_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('IFR_FALTA') is None:
        transformed_row['IFR_FALTA'] = int(transformed_row.get('IFR_FALTA'))
    if not transformed_row.get('IFR_ID') is None:
        transformed_row['IFR_ID'] = int(transformed_row.get('IFR_ID'))
    if not transformed_row.get('IFR_MES') is None:
        transformed_row['IFR_MES'] = int(transformed_row.get('IFR_MES'))
    if not transformed_row.get('IFR_OLD_ID') is None:
        transformed_row['IFR_OLD_ID'] = int(transformed_row.get('IFR_OLD_ID'))
    if not transformed_row.get('IFR_SCHOOL_CLASS_ID') is None:
        transformed_row['IFR_SCHOOL_CLASS_ID'] = int(transformed_row.get('IFR_SCHOOL_CLASS_ID'))

    return transformed_row

def transform_job_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('assessmentId') is None:
        transformed_row['assessmentId'] = int(transformed_row.get('assessmentId'))
    if not transformed_row.get('bullId') is None:
        transformed_row['bullId'] = str(transformed_row.get('bullId'))
    if not transformed_row.get('countyId') is None:
        transformed_row['countyId'] = int(transformed_row.get('countyId'))
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('endDate') is None:
        transformed_row['endDate'] = converter_data(
            valor_data=transformed_row.get('endDate'),
            formato_entrada='%Y-%m-%d %H:%M:%S',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('jobType') is None:
        transformed_row['jobType'] = str(transformed_row.get('jobType'))
    if not transformed_row.get('startDate') is None:
        transformed_row['startDate'] = converter_data(
            valor_data=transformed_row.get('startDate'),
            formato_entrada='%Y-%m-%d %H:%M:%S',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_matriz_referencia_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('MAR_ATIVO') is None:
        transformed_row['MAR_ATIVO'] = int(transformed_row.get('MAR_ATIVO'))
    if not transformed_row.get('MAR_DIS_ID') is None:
        transformed_row['MAR_DIS_ID'] = int(transformed_row.get('MAR_DIS_ID'))
    if not transformed_row.get('MAR_DT_ATUALIZACAO') is None:
        transformed_row['MAR_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('MAR_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MAR_DT_CRIACAO') is None:
        transformed_row['MAR_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('MAR_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MAR_ID') is None:
        transformed_row['MAR_ID'] = int(transformed_row.get('MAR_ID'))
    if not transformed_row.get('MAR_NOME') is None:
        transformed_row['MAR_NOME'] = str(transformed_row.get('MAR_NOME'))
    if not transformed_row.get('MAR_OLD_ID') is None:
        transformed_row['MAR_OLD_ID'] = int(transformed_row.get('MAR_OLD_ID'))

    return transformed_row

def transform_matriz_referencia_serie_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('MAR_ID') is None:
        transformed_row['MAR_ID'] = int(transformed_row.get('MAR_ID'))
    if not transformed_row.get('SER_ID') is None:
        transformed_row['SER_ID'] = int(transformed_row.get('SER_ID'))

    return transformed_row

def transform_matriz_referencia_topico_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('MTO_ATIVO') is None:
        transformed_row['MTO_ATIVO'] = int(transformed_row.get('MTO_ATIVO'))
    if not transformed_row.get('MTO_DT_ATUALIZACAO') is None:
        transformed_row['MTO_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('MTO_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MTO_DT_CRIACAO') is None:
        transformed_row['MTO_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('MTO_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MTO_ID') is None:
        transformed_row['MTO_ID'] = int(transformed_row.get('MTO_ID'))
    if not transformed_row.get('MTO_MAR_ID') is None:
        transformed_row['MTO_MAR_ID'] = int(transformed_row.get('MTO_MAR_ID'))
    if not transformed_row.get('MTO_NOME') is None:
        transformed_row['MTO_NOME'] = str(transformed_row.get('MTO_NOME'))
    if not transformed_row.get('MTO_OLD_ID') is None:
        transformed_row['MTO_OLD_ID'] = int(transformed_row.get('MTO_OLD_ID'))

    return transformed_row

def transform_matriz_referencia_topico_items_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('MTI_ATIVO') is None:
        transformed_row['MTI_ATIVO'] = int(transformed_row.get('MTI_ATIVO'))
    if not transformed_row.get('MTI_CODIGO') is None:
        transformed_row['MTI_CODIGO'] = str(transformed_row.get('MTI_CODIGO'))
    if not transformed_row.get('MTI_DESCRITOR') is None:
        transformed_row['MTI_DESCRITOR'] = str(transformed_row.get('MTI_DESCRITOR'))
    if not transformed_row.get('MTI_DT_ATUALIZACAO') is None:
        transformed_row['MTI_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('MTI_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MTI_DT_CRIACAO') is None:
        transformed_row['MTI_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('MTI_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MTI_ID') is None:
        transformed_row['MTI_ID'] = int(transformed_row.get('MTI_ID'))
    if not transformed_row.get('MTI_MTO_ID') is None:
        transformed_row['MTI_MTO_ID'] = int(transformed_row.get('MTI_MTO_ID'))
    if not transformed_row.get('MTI_OLD_ID') is None:
        transformed_row['MTI_OLD_ID'] = int(transformed_row.get('MTI_OLD_ID'))

    return transformed_row

def transform_messages_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('MEN_DT_ATUALIZACAO') is None:
        transformed_row['MEN_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('MEN_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MEN_DT_CRIACAO') is None:
        transformed_row['MEN_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('MEN_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MEN_ID') is None:
        transformed_row['MEN_ID'] = int(transformed_row.get('MEN_ID'))
    if not transformed_row.get('MEN_IS_DELETE') is None:
        transformed_row['MEN_IS_DELETE'] = int(transformed_row.get('MEN_IS_DELETE'))
    if not transformed_row.get('MEN_TEXT') is None:
        transformed_row['MEN_TEXT'] = str(transformed_row.get('MEN_TEXT'))
    if not transformed_row.get('MEN_TITLE') is None:
        transformed_row['MEN_TITLE'] = str(transformed_row.get('MEN_TITLE'))

    return transformed_row

def transform_messages_municipios_municipio_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('messagesMENID') is None:
        transformed_row['messagesMENID'] = int(transformed_row.get('messagesMENID'))
    if not transformed_row.get('municipioMUNID') is None:
        transformed_row['municipioMUNID'] = int(transformed_row.get('municipioMUNID'))

    return transformed_row

def transform_messages_schools_escola_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('escolaESCID') is None:
        transformed_row['escolaESCID'] = int(transformed_row.get('escolaESCID'))
    if not transformed_row.get('messagesMENID') is None:
        transformed_row['messagesMENID'] = int(transformed_row.get('messagesMENID'))

    return transformed_row

def transform_migrations_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('name') is None:
        transformed_row['name'] = str(transformed_row.get('name'))
    if not transformed_row.get('timestamp') is None:
        transformed_row['timestamp'] = int(transformed_row.get('timestamp'))

    return transformed_row

def transform_municipio_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('MUN_ARQ_CONVENIO') is None:
        transformed_row['MUN_ARQ_CONVENIO'] = str(transformed_row.get('MUN_ARQ_CONVENIO'))
    if not transformed_row.get('MUN_ATIVO') is None:
        transformed_row['MUN_ATIVO'] = int(transformed_row.get('MUN_ATIVO'))
    if not transformed_row.get('MUN_BAIRRO') is None:
        transformed_row['MUN_BAIRRO'] = str(transformed_row.get('MUN_BAIRRO'))
    if not transformed_row.get('MUN_CEP') is None:
        transformed_row['MUN_CEP'] = str(transformed_row.get('MUN_CEP'))
    if not transformed_row.get('MUN_CIDADE') is None:
        transformed_row['MUN_CIDADE'] = str(transformed_row.get('MUN_CIDADE'))
    if not transformed_row.get('MUN_COD_IBGE') is None:
        transformed_row['MUN_COD_IBGE'] = int(transformed_row.get('MUN_COD_IBGE'))
    if not transformed_row.get('MUN_COMPARTILHAR_DADOS') is None:
        transformed_row['MUN_COMPARTILHAR_DADOS'] = int(transformed_row.get('MUN_COMPARTILHAR_DADOS'))
    if not transformed_row.get('MUN_COMPLEMENTO') is None:
        transformed_row['MUN_COMPLEMENTO'] = str(transformed_row.get('MUN_COMPLEMENTO'))
    if not transformed_row.get('MUN_DT_ATUALIZACAO') is None:
        transformed_row['MUN_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('MUN_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MUN_DT_CRIACAO') is None:
        transformed_row['MUN_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('MUN_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MUN_DT_FIM') is None:
        transformed_row['MUN_DT_FIM'] = converter_data(
            valor_data=transformed_row.get('MUN_DT_FIM'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MUN_DT_INICIO') is None:
        transformed_row['MUN_DT_INICIO'] = converter_data(
            valor_data=transformed_row.get('MUN_DT_INICIO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('MUN_ENDERECO') is None:
        transformed_row['MUN_ENDERECO'] = str(transformed_row.get('MUN_ENDERECO'))
    if not transformed_row.get('MUN_ID') is None:
        transformed_row['MUN_ID'] = int(transformed_row.get('MUN_ID'))
    if not transformed_row.get('MUN_LOGO') is None:
        transformed_row['MUN_LOGO'] = str(transformed_row.get('MUN_LOGO'))
    if not transformed_row.get('MUN_MENSAGEM_EMAIL_ATIVO') is None:
        transformed_row['MUN_MENSAGEM_EMAIL_ATIVO'] = int(transformed_row.get('MUN_MENSAGEM_EMAIL_ATIVO'))
    if not transformed_row.get('MUN_MENSAGEM_WHATSAPP_ATIVO') is None:
        transformed_row['MUN_MENSAGEM_WHATSAPP_ATIVO'] = int(transformed_row.get('MUN_MENSAGEM_WHATSAPP_ATIVO'))
    if not transformed_row.get('MUN_NOME') is None:
        transformed_row['MUN_NOME'] = str(transformed_row.get('MUN_NOME'))
    if not transformed_row.get('MUN_NUMERO') is None:
        transformed_row['MUN_NUMERO'] = str(transformed_row.get('MUN_NUMERO'))
    if not transformed_row.get('MUN_OLD_ID') is None:
        transformed_row['MUN_OLD_ID'] = int(transformed_row.get('MUN_OLD_ID'))
    if not transformed_row.get('MUN_PARCEIRO_EPV') is None:
        transformed_row['MUN_PARCEIRO_EPV'] = int(transformed_row.get('MUN_PARCEIRO_EPV'))
    if not transformed_row.get('MUN_STATUS') is None:
        transformed_row['MUN_STATUS'] = str(transformed_row.get('MUN_STATUS'))
    if not transformed_row.get('MUN_UF') is None:
        transformed_row['MUN_UF'] = str(transformed_row.get('MUN_UF'))
    if not transformed_row.get('stateId') is None:
        transformed_row['stateId'] = int(transformed_row.get('stateId'))
    if not transformed_row.get('stateRegionalId') is None:
        transformed_row['stateRegionalId'] = int(transformed_row.get('stateRegionalId'))

    return transformed_row

def transform_pcd_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('PCD_ATIVO') is None:
        transformed_row['PCD_ATIVO'] = int(transformed_row.get('PCD_ATIVO'))
    if not transformed_row.get('PCD_DT_ATUALIZACAO') is None:
        transformed_row['PCD_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('PCD_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('PCD_DT_CRIACAO') is None:
        transformed_row['PCD_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('PCD_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('PCD_ID') is None:
        transformed_row['PCD_ID'] = int(transformed_row.get('PCD_ID'))
    if not transformed_row.get('PCD_NOME') is None:
        transformed_row['PCD_NOME'] = str(transformed_row.get('PCD_NOME'))
    if not transformed_row.get('PCD_OLD_ID') is None:
        transformed_row['PCD_OLD_ID'] = int(transformed_row.get('PCD_OLD_ID'))

    return transformed_row

def transform_perfil_base_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('PER_ATIVO') is None:
        transformed_row['PER_ATIVO'] = int(transformed_row.get('PER_ATIVO'))
    if not transformed_row.get('PER_DT_ATUALIZACAO') is None:
        transformed_row['PER_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('PER_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('PER_DT_CRIACAO') is None:
        transformed_row['PER_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('PER_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('PER_ID') is None:
        transformed_row['PER_ID'] = int(transformed_row.get('PER_ID'))
    if not transformed_row.get('PER_NOME') is None:
        transformed_row['PER_NOME'] = str(transformed_row.get('PER_NOME'))

    return transformed_row

def transform_professor_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('PRO_ATIVO') is None:
        transformed_row['PRO_ATIVO'] = int(transformed_row.get('PRO_ATIVO'))
    if not transformed_row.get('PRO_AVATAR') is None:
        transformed_row['PRO_AVATAR'] = str(transformed_row.get('PRO_AVATAR'))
    if not transformed_row.get('PRO_BAIRRO') is None:
        transformed_row['PRO_BAIRRO'] = str(transformed_row.get('PRO_BAIRRO'))
    if not transformed_row.get('PRO_CEP') is None:
        transformed_row['PRO_CEP'] = str(transformed_row.get('PRO_CEP'))
    if not transformed_row.get('PRO_CIDADE') is None:
        transformed_row['PRO_CIDADE'] = str(transformed_row.get('PRO_CIDADE'))
    if not transformed_row.get('PRO_COMPLEMENTO') is None:
        transformed_row['PRO_COMPLEMENTO'] = str(transformed_row.get('PRO_COMPLEMENTO'))
    if not transformed_row.get('PRO_DOCUMENTO') is None:
        transformed_row['PRO_DOCUMENTO'] = str(transformed_row.get('PRO_DOCUMENTO'))
    if not transformed_row.get('PRO_DT_ATUALIZACAO') is None:
        transformed_row['PRO_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('PRO_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('PRO_DT_CRIACAO') is None:
        transformed_row['PRO_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('PRO_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('PRO_DT_NASC') is None:
        transformed_row['PRO_DT_NASC'] = converter_data(
            valor_data=transformed_row.get('PRO_DT_NASC'),
            formato_entrada='%Y-%m-%d %H:%M:%S',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('PRO_EMAIL') is None:
        transformed_row['PRO_EMAIL'] = str(transformed_row.get('PRO_EMAIL'))
    if not transformed_row.get('PRO_ENDERECO') is None:
        transformed_row['PRO_ENDERECO'] = str(transformed_row.get('PRO_ENDERECO'))
    if not transformed_row.get('PRO_FONE') is None:
        transformed_row['PRO_FONE'] = str(transformed_row.get('PRO_FONE'))
    if not transformed_row.get('PRO_FOR_ID') is None:
        transformed_row['PRO_FOR_ID'] = int(transformed_row.get('PRO_FOR_ID'))
    if not transformed_row.get('PRO_GEN_ID') is None:
        transformed_row['PRO_GEN_ID'] = int(transformed_row.get('PRO_GEN_ID'))
    if not transformed_row.get('PRO_ID') is None:
        transformed_row['PRO_ID'] = int(transformed_row.get('PRO_ID'))
    if not transformed_row.get('PRO_MUN_ID') is None:
        transformed_row['PRO_MUN_ID'] = int(transformed_row.get('PRO_MUN_ID'))
    if not transformed_row.get('PRO_NOME') is None:
        transformed_row['PRO_NOME'] = str(transformed_row.get('PRO_NOME'))
    if not transformed_row.get('PRO_NUMERO') is None:
        transformed_row['PRO_NUMERO'] = str(transformed_row.get('PRO_NUMERO'))
    if not transformed_row.get('PRO_OLD_ID') is None:
        transformed_row['PRO_OLD_ID'] = int(transformed_row.get('PRO_OLD_ID'))
    if not transformed_row.get('PRO_PEL_ID') is None:
        transformed_row['PRO_PEL_ID'] = int(transformed_row.get('PRO_PEL_ID'))
    if not transformed_row.get('PRO_UF') is None:
        transformed_row['PRO_UF'] = str(transformed_row.get('PRO_UF'))

    return transformed_row

def transform_raca_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('PEL_ATIVO') is None:
        transformed_row['PEL_ATIVO'] = int(transformed_row.get('PEL_ATIVO'))
    if not transformed_row.get('PEL_DT_ATUALIZACAO') is None:
        transformed_row['PEL_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('PEL_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('PEL_DT_CRIACAO') is None:
        transformed_row['PEL_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('PEL_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('PEL_ID') is None:
        transformed_row['PEL_ID'] = int(transformed_row.get('PEL_ID'))
    if not transformed_row.get('PEL_NOME') is None:
        transformed_row['PEL_NOME'] = str(transformed_row.get('PEL_NOME'))
    if not transformed_row.get('PEL_OLD_ID') is None:
        transformed_row['PEL_OLD_ID'] = int(transformed_row.get('PEL_OLD_ID'))

    return transformed_row

def transform_regionais_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('active') is None:
        transformed_row['active'] = int(transformed_row.get('active'))
    if not transformed_row.get('countyId') is None:
        transformed_row['countyId'] = int(transformed_row.get('countyId'))
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('name') is None:
        transformed_row['name'] = str(transformed_row.get('name'))
    if not transformed_row.get('stateId') is None:
        transformed_row['stateId'] = int(transformed_row.get('stateId'))
    if not transformed_row.get('type') is None:
        transformed_row['type'] = str(transformed_row.get('type'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_report_descriptor_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('descriptorMTIID') is None:
        transformed_row['descriptorMTIID'] = int(transformed_row.get('descriptorMTIID'))
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('reportEditionId') is None:
        transformed_row['reportEditionId'] = int(transformed_row.get('reportEditionId'))
    if not transformed_row.get('testTESID') is None:
        transformed_row['testTESID'] = int(transformed_row.get('testTESID'))
    if not transformed_row.get('total') is None:
        transformed_row['total'] = int(transformed_row.get('total'))
    if not transformed_row.get('totalCorrect') is None:
        transformed_row['totalCorrect'] = int(transformed_row.get('totalCorrect'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_report_edition_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('countyMUNID') is None:
        transformed_row['countyMUNID'] = int(transformed_row.get('countyMUNID'))
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('editionAVAID') is None:
        transformed_row['editionAVAID'] = int(transformed_row.get('editionAVAID'))
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('regionalId') is None:
        transformed_row['regionalId'] = int(transformed_row.get('regionalId'))
    if not transformed_row.get('schoolClassTURID') is None:
        transformed_row['schoolClassTURID'] = int(transformed_row.get('schoolClassTURID'))
    if not transformed_row.get('schoolESCID') is None:
        transformed_row['schoolESCID'] = int(transformed_row.get('schoolESCID'))
    if not transformed_row.get('type') is None:
        transformed_row['type'] = str(transformed_row.get('type'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_report_not_evaluated_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('abandono') is None:
        transformed_row['abandono'] = int(transformed_row.get('abandono'))
    if not transformed_row.get('ausencia') is None:
        transformed_row['ausencia'] = int(transformed_row.get('ausencia'))
    if not transformed_row.get('countPresentStudents') is None:
        transformed_row['countPresentStudents'] = int(transformed_row.get('countPresentStudents'))
    if not transformed_row.get('countStudentsLaunched') is None:
        transformed_row['countStudentsLaunched'] = int(transformed_row.get('countStudentsLaunched'))
    if not transformed_row.get('countTotalStudents') is None:
        transformed_row['countTotalStudents'] = int(transformed_row.get('countTotalStudents'))
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('deficiencia') is None:
        transformed_row['deficiencia'] = int(transformed_row.get('deficiencia'))
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('idStudents') is None:
        transformed_row['idStudents'] = str(transformed_row.get('idStudents'))
    if not transformed_row.get('name') is None:
        transformed_row['name'] = str(transformed_row.get('name'))
    if not transformed_row.get('nao_participou') is None:
        transformed_row['nao_participou'] = int(transformed_row.get('nao_participou'))
    if not transformed_row.get('recusa') is None:
        transformed_row['recusa'] = int(transformed_row.get('recusa'))
    if not transformed_row.get('reportEditionId') is None:
        transformed_row['reportEditionId'] = int(transformed_row.get('reportEditionId'))
    if not transformed_row.get('testTESID') is None:
        transformed_row['testTESID'] = int(transformed_row.get('testTESID'))
    if not transformed_row.get('transferencia') is None:
        transformed_row['transferencia'] = int(transformed_row.get('transferencia'))
    if not transformed_row.get('type') is None:
        transformed_row['type'] = str(transformed_row.get('type'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_report_question_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('fluente') is None:
        transformed_row['fluente'] = int(transformed_row.get('fluente'))
    if not transformed_row.get('frases') is None:
        transformed_row['frases'] = int(transformed_row.get('frases'))
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('nao_avaliado') is None:
        transformed_row['nao_avaliado'] = int(transformed_row.get('nao_avaliado'))
    if not transformed_row.get('nao_fluente') is None:
        transformed_row['nao_fluente'] = int(transformed_row.get('nao_fluente'))
    if not transformed_row.get('nao_informado') is None:
        transformed_row['nao_informado'] = int(transformed_row.get('nao_informado'))
    if not transformed_row.get('nao_leitor') is None:
        transformed_row['nao_leitor'] = int(transformed_row.get('nao_leitor'))
    if not transformed_row.get('option_correct') is None:
        transformed_row['option_correct'] = str(transformed_row.get('option_correct'))
    if not transformed_row.get('palavras') is None:
        transformed_row['palavras'] = int(transformed_row.get('palavras'))
    if not transformed_row.get('questionTEGID') is None:
        transformed_row['questionTEGID'] = int(transformed_row.get('questionTEGID'))
    if not transformed_row.get('reportSubjectId') is None:
        transformed_row['reportSubjectId'] = int(transformed_row.get('reportSubjectId'))
    if not transformed_row.get('silabas') is None:
        transformed_row['silabas'] = int(transformed_row.get('silabas'))
    if not transformed_row.get('total_a') is None:
        transformed_row['total_a'] = int(transformed_row.get('total_a'))
    if not transformed_row.get('total_b') is None:
        transformed_row['total_b'] = int(transformed_row.get('total_b'))
    if not transformed_row.get('total_c') is None:
        transformed_row['total_c'] = int(transformed_row.get('total_c'))
    if not transformed_row.get('total_d') is None:
        transformed_row['total_d'] = int(transformed_row.get('total_d'))
    if not transformed_row.get('total_null') is None:
        transformed_row['total_null'] = int(transformed_row.get('total_null'))

    return transformed_row

def transform_report_race_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('countPresentStudents') is None:
        transformed_row['countPresentStudents'] = int(transformed_row.get('countPresentStudents'))
    if not transformed_row.get('countStudentsLaunched') is None:
        transformed_row['countStudentsLaunched'] = int(transformed_row.get('countStudentsLaunched'))
    if not transformed_row.get('countTotalStudents') is None:
        transformed_row['countTotalStudents'] = int(transformed_row.get('countTotalStudents'))
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('fluente') is None:
        transformed_row['fluente'] = int(transformed_row.get('fluente'))
    if not transformed_row.get('frases') is None:
        transformed_row['frases'] = int(transformed_row.get('frases'))
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('idStudents') is None:
        transformed_row['idStudents'] = str(transformed_row.get('idStudents'))
    if not transformed_row.get('name') is None:
        transformed_row['name'] = str(transformed_row.get('name'))
    if not transformed_row.get('nao_avaliado') is None:
        transformed_row['nao_avaliado'] = int(transformed_row.get('nao_avaliado'))
    if not transformed_row.get('nao_fluente') is None:
        transformed_row['nao_fluente'] = int(transformed_row.get('nao_fluente'))
    if not transformed_row.get('nao_informado') is None:
        transformed_row['nao_informado'] = int(transformed_row.get('nao_informado'))
    if not transformed_row.get('nao_leitor') is None:
        transformed_row['nao_leitor'] = int(transformed_row.get('nao_leitor'))
    if not transformed_row.get('palavras') is None:
        transformed_row['palavras'] = int(transformed_row.get('palavras'))
    if not transformed_row.get('racePELID') is None:
        transformed_row['racePELID'] = int(transformed_row.get('racePELID'))
    if not transformed_row.get('reportSubjectId') is None:
        transformed_row['reportSubjectId'] = int(transformed_row.get('reportSubjectId'))
    if not transformed_row.get('silabas') is None:
        transformed_row['silabas'] = int(transformed_row.get('silabas'))
    if not transformed_row.get('totalGradesStudents') is None:
        transformed_row['totalGradesStudents'] = int(transformed_row.get('totalGradesStudents'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_report_subject_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('countPresentStudents') is None:
        transformed_row['countPresentStudents'] = int(transformed_row.get('countPresentStudents'))
    if not transformed_row.get('countStudentsLaunched') is None:
        transformed_row['countStudentsLaunched'] = int(transformed_row.get('countStudentsLaunched'))
    if not transformed_row.get('countTotalStudents') is None:
        transformed_row['countTotalStudents'] = int(transformed_row.get('countTotalStudents'))
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('fluente') is None:
        transformed_row['fluente'] = int(transformed_row.get('fluente'))
    if not transformed_row.get('frases') is None:
        transformed_row['frases'] = int(transformed_row.get('frases'))
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('idStudents') is None:
        transformed_row['idStudents'] = str(transformed_row.get('idStudents'))
    if not transformed_row.get('name') is None:
        transformed_row['name'] = str(transformed_row.get('name'))
    if not transformed_row.get('nao_avaliado') is None:
        transformed_row['nao_avaliado'] = int(transformed_row.get('nao_avaliado'))
    if not transformed_row.get('nao_fluente') is None:
        transformed_row['nao_fluente'] = int(transformed_row.get('nao_fluente'))
    if not transformed_row.get('nao_informado') is None:
        transformed_row['nao_informado'] = int(transformed_row.get('nao_informado'))
    if not transformed_row.get('nao_leitor') is None:
        transformed_row['nao_leitor'] = int(transformed_row.get('nao_leitor'))
    if not transformed_row.get('palavras') is None:
        transformed_row['palavras'] = int(transformed_row.get('palavras'))
    if not transformed_row.get('reportEditionId') is None:
        transformed_row['reportEditionId'] = int(transformed_row.get('reportEditionId'))
    if not transformed_row.get('silabas') is None:
        transformed_row['silabas'] = int(transformed_row.get('silabas'))
    if not transformed_row.get('testTESID') is None:
        transformed_row['testTESID'] = int(transformed_row.get('testTESID'))
    if not transformed_row.get('totalGradesStudents') is None:
        transformed_row['totalGradesStudents'] = int(transformed_row.get('totalGradesStudents'))
    if not transformed_row.get('type') is None:
        transformed_row['type'] = str(transformed_row.get('type'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%dT%H:%M:%S.%f',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_series_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('SER_ATIVO') is None:
        transformed_row['SER_ATIVO'] = int(transformed_row.get('SER_ATIVO'))
    if not transformed_row.get('SER_DT_ATUALIZACAO') is None:
        transformed_row['SER_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('SER_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('SER_DT_CRIACAO') is None:
        transformed_row['SER_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('SER_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('SER_ID') is None:
        transformed_row['SER_ID'] = int(transformed_row.get('SER_ID'))
    if not transformed_row.get('SER_NOME') is None:
        transformed_row['SER_NOME'] = str(transformed_row.get('SER_NOME'))
    if not transformed_row.get('SER_NUMBER') is None:
        transformed_row['SER_NUMBER'] = int(transformed_row.get('SER_NUMBER'))
    if not transformed_row.get('SER_OLD_ID') is None:
        transformed_row['SER_OLD_ID'] = int(transformed_row.get('SER_OLD_ID'))

    return transformed_row

def transform_sub_perfil_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('SPE_ATIVO') is None:
        transformed_row['SPE_ATIVO'] = int(transformed_row.get('SPE_ATIVO'))
    if not transformed_row.get('SPE_DT_ATUALIZACAO') is None:
        transformed_row['SPE_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('SPE_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('SPE_DT_CRIACAO') is None:
        transformed_row['SPE_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('SPE_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('SPE_ID') is None:
        transformed_row['SPE_ID'] = int(transformed_row.get('SPE_ID'))
    if not transformed_row.get('SPE_NOME') is None:
        transformed_row['SPE_NOME'] = str(transformed_row.get('SPE_NOME'))
    if not transformed_row.get('SPE_PER_ID') is None:
        transformed_row['SPE_PER_ID'] = int(transformed_row.get('SPE_PER_ID'))
    if not transformed_row.get('role') is None:
        transformed_row['role'] = str(transformed_row.get('role'))

    return transformed_row

def transform_sub_perfil_area_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('ARE_ID') is None:
        transformed_row['ARE_ID'] = int(transformed_row.get('ARE_ID'))
    if not transformed_row.get('SPE_ID') is None:
        transformed_row['SPE_ID'] = int(transformed_row.get('SPE_ID'))

    return transformed_row

def transform_teste_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('TES_ANO') is None:
        transformed_row['TES_ANO'] = str(transformed_row.get('TES_ANO'))
    if not transformed_row.get('TES_ARQUIVO') is None:
        transformed_row['TES_ARQUIVO'] = str(transformed_row.get('TES_ARQUIVO'))
    if not transformed_row.get('TES_ATIVO') is None:
        transformed_row['TES_ATIVO'] = int(transformed_row.get('TES_ATIVO'))
    if not transformed_row.get('TES_DIS_ID') is None:
        transformed_row['TES_DIS_ID'] = int(transformed_row.get('TES_DIS_ID'))
    if not transformed_row.get('TES_DT_ATUALIZACAO') is None:
        transformed_row['TES_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('TES_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('TES_DT_CRIACAO') is None:
        transformed_row['TES_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('TES_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('TES_ID') is None:
        transformed_row['TES_ID'] = int(transformed_row.get('TES_ID'))
    if not transformed_row.get('TES_MANUAL') is None:
        transformed_row['TES_MANUAL'] = str(transformed_row.get('TES_MANUAL'))
    if not transformed_row.get('TES_MAR_ID') is None:
        transformed_row['TES_MAR_ID'] = int(transformed_row.get('TES_MAR_ID'))
    if not transformed_row.get('TES_NOME') is None:
        transformed_row['TES_NOME'] = str(transformed_row.get('TES_NOME'))
    if not transformed_row.get('TES_OLD_ID') is None:
        transformed_row['TES_OLD_ID'] = int(transformed_row.get('TES_OLD_ID'))
    if not transformed_row.get('TES_SER_ID') is None:
        transformed_row['TES_SER_ID'] = int(transformed_row.get('TES_SER_ID'))
    if not transformed_row.get('assessmentOnlineId') is None:
        transformed_row['assessmentOnlineId'] = int(transformed_row.get('assessmentOnlineId'))

    return transformed_row

def transform_teste_gabarito_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('TEG_DT_ATUALIZACAO') is None:
        transformed_row['TEG_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('TEG_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('TEG_DT_CRIACAO') is None:
        transformed_row['TEG_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('TEG_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('TEG_ID') is None:
        transformed_row['TEG_ID'] = int(transformed_row.get('TEG_ID'))
    if not transformed_row.get('TEG_MTI_ID') is None:
        transformed_row['TEG_MTI_ID'] = int(transformed_row.get('TEG_MTI_ID'))
    if not transformed_row.get('TEG_OLD_ID') is None:
        transformed_row['TEG_OLD_ID'] = int(transformed_row.get('TEG_OLD_ID'))
    if not transformed_row.get('TEG_ORDEM') is None:
        transformed_row['TEG_ORDEM'] = int(transformed_row.get('TEG_ORDEM'))
    if not transformed_row.get('TEG_RESPOSTA_CORRETA') is None:
        transformed_row['TEG_RESPOSTA_CORRETA'] = str(transformed_row.get('TEG_RESPOSTA_CORRETA'))
    if not transformed_row.get('TEG_TES_ID') is None:
        transformed_row['TEG_TES_ID'] = int(transformed_row.get('TEG_TES_ID'))

    return transformed_row

def transform_transferencia_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('TRF_ALU_ID') is None:
        transformed_row['TRF_ALU_ID'] = int(transformed_row.get('TRF_ALU_ID'))
    if not transformed_row.get('TRF_DT_ATUALIZACAO') is None:
        transformed_row['TRF_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('TRF_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('TRF_DT_CRIACAO') is None:
        transformed_row['TRF_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('TRF_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('TRF_ESC_ID_DESTINO') is None:
        transformed_row['TRF_ESC_ID_DESTINO'] = int(transformed_row.get('TRF_ESC_ID_DESTINO'))
    if not transformed_row.get('TRF_ESC_ID_ORIGEM') is None:
        transformed_row['TRF_ESC_ID_ORIGEM'] = int(transformed_row.get('TRF_ESC_ID_ORIGEM'))
    if not transformed_row.get('TRF_ID') is None:
        transformed_row['TRF_ID'] = int(transformed_row.get('TRF_ID'))
    if not transformed_row.get('TRF_JUSTIFICATIVA') is None:
        transformed_row['TRF_JUSTIFICATIVA'] = str(transformed_row.get('TRF_JUSTIFICATIVA'))
    if not transformed_row.get('TRF_OLD_ID') is None:
        transformed_row['TRF_OLD_ID'] = int(transformed_row.get('TRF_OLD_ID'))
    if not transformed_row.get('TRF_STATUS') is None:
        transformed_row['TRF_STATUS'] = str(transformed_row.get('TRF_STATUS'))
    if not transformed_row.get('TRF_TUR_ID_DESTINO') is None:
        transformed_row['TRF_TUR_ID_DESTINO'] = int(transformed_row.get('TRF_TUR_ID_DESTINO'))
    if not transformed_row.get('TRF_TUR_ID_ORIGEM') is None:
        transformed_row['TRF_TUR_ID_ORIGEM'] = int(transformed_row.get('TRF_TUR_ID_ORIGEM'))
    if not transformed_row.get('TRF_USU') is None:
        transformed_row['TRF_USU'] = int(transformed_row.get('TRF_USU'))
    if not transformed_row.get('TRF_USU_STATUS') is None:
        transformed_row['TRF_USU_STATUS'] = int(transformed_row.get('TRF_USU_STATUS'))

    return transformed_row

def transform_turma_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('TUR_ANEXO') is None:
        transformed_row['TUR_ANEXO'] = int(transformed_row.get('TUR_ANEXO'))
    if not transformed_row.get('TUR_ANO') is None:
        transformed_row['TUR_ANO'] = str(transformed_row.get('TUR_ANO'))
    if not transformed_row.get('TUR_ATIVO') is None:
        transformed_row['TUR_ATIVO'] = int(transformed_row.get('TUR_ATIVO'))
    if not transformed_row.get('TUR_DT_ATUALIZACAO') is None:
        transformed_row['TUR_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('TUR_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('TUR_DT_CRIACAO') is None:
        transformed_row['TUR_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('TUR_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('TUR_ESC_ID') is None:
        transformed_row['TUR_ESC_ID'] = int(transformed_row.get('TUR_ESC_ID'))
    if not transformed_row.get('TUR_ID') is None:
        transformed_row['TUR_ID'] = int(transformed_row.get('TUR_ID'))
    if not transformed_row.get('TUR_MUN_ID') is None:
        transformed_row['TUR_MUN_ID'] = int(transformed_row.get('TUR_MUN_ID'))
    if not transformed_row.get('TUR_NOME') is None:
        transformed_row['TUR_NOME'] = str(transformed_row.get('TUR_NOME'))
    if not transformed_row.get('TUR_OLD_ID') is None:
        transformed_row['TUR_OLD_ID'] = int(transformed_row.get('TUR_OLD_ID'))
    if not transformed_row.get('TUR_PERIODO') is None:
        transformed_row['TUR_PERIODO'] = str(transformed_row.get('TUR_PERIODO'))
    if not transformed_row.get('TUR_SER_ID') is None:
        transformed_row['TUR_SER_ID'] = int(transformed_row.get('TUR_SER_ID'))
    if not transformed_row.get('TUR_TIPO') is None:
        transformed_row['TUR_TIPO'] = str(transformed_row.get('TUR_TIPO'))

    return transformed_row

def transform_turma_aluno_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('createdAt') is None:
        transformed_row['createdAt'] = converter_data(
            valor_data=transformed_row.get('createdAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('endDate') is None:
        transformed_row['endDate'] = converter_data(
            valor_data=transformed_row.get('endDate'),
            formato_entrada='%Y-%m-%d',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('id') is None:
        transformed_row['id'] = int(transformed_row.get('id'))
    if not transformed_row.get('schoolClassTURID') is None:
        transformed_row['schoolClassTURID'] = int(transformed_row.get('schoolClassTURID'))
    if not transformed_row.get('startDate') is None:
        transformed_row['startDate'] = converter_data(
            valor_data=transformed_row.get('startDate'),
            formato_entrada='%Y-%m-%d',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('studentALUID') is None:
        transformed_row['studentALUID'] = int(transformed_row.get('studentALUID'))
    if not transformed_row.get('updatedAt') is None:
        transformed_row['updatedAt'] = converter_data(
            valor_data=transformed_row.get('updatedAt'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )

    return transformed_row

def transform_turma_professor_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('PRO_ID') is None:
        transformed_row['PRO_ID'] = int(transformed_row.get('PRO_ID'))
    if not transformed_row.get('TUR_ID') is None:
        transformed_row['TUR_ID'] = int(transformed_row.get('TUR_ID'))

    return transformed_row

def transform_usuario_table(row_dict):
    transformed_row = row_dict.copy()
        
    if not transformed_row.get('USU_ATIVO') is None:
        transformed_row['USU_ATIVO'] = int(transformed_row.get('USU_ATIVO'))
    if not transformed_row.get('USU_AVATAR') is None:
        transformed_row['USU_AVATAR'] = str(transformed_row.get('USU_AVATAR'))
    if not transformed_row.get('USU_DOCUMENTO') is None:
        transformed_row['USU_DOCUMENTO'] = str(transformed_row.get('USU_DOCUMENTO'))
    if not transformed_row.get('USU_DT_ATUALIZACAO') is None:
        transformed_row['USU_DT_ATUALIZACAO'] = converter_data(
            valor_data=transformed_row.get('USU_DT_ATUALIZACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('USU_DT_CRIACAO') is None:
        transformed_row['USU_DT_CRIACAO'] = converter_data(
            valor_data=transformed_row.get('USU_DT_CRIACAO'),
            formato_entrada='%Y-%m-%d %H:%M:%S.%f %Z',
            formato_saida='%Y-%m-%d %H:%M:%S.%f' # Formato DATETIME para o BigQuery
        )
    if not transformed_row.get('USU_EMAIL') is None:
        transformed_row['USU_EMAIL'] = str(transformed_row.get('USU_EMAIL'))
    if not transformed_row.get('USU_ESC_ID') is None:
        transformed_row['USU_ESC_ID'] = int(transformed_row.get('USU_ESC_ID'))
    if not transformed_row.get('USU_FONE') is None:
        transformed_row['USU_FONE'] = str(transformed_row.get('USU_FONE'))
    if not transformed_row.get('USU_ID') is None:
        transformed_row['USU_ID'] = int(transformed_row.get('USU_ID'))
    if not transformed_row.get('USU_MUN_ID') is None:
        transformed_row['USU_MUN_ID'] = int(transformed_row.get('USU_MUN_ID'))
    if not transformed_row.get('USU_NOME') is None:
        transformed_row['USU_NOME'] = str(transformed_row.get('USU_NOME'))
    if not transformed_row.get('USU_SENHA') is None:
        transformed_row['USU_SENHA'] = str(transformed_row.get('USU_SENHA'))
    if not transformed_row.get('USU_SPE_ID') is None:
        transformed_row['USU_SPE_ID'] = int(transformed_row.get('USU_SPE_ID'))
    if not transformed_row.get('isChangePasswordWelcome') is None:
        transformed_row['isChangePasswordWelcome'] = int(transformed_row.get('isChangePasswordWelcome'))
    if not transformed_row.get('stateId') is None:
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