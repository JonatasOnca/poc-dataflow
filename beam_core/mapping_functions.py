# Copyright 2025 TecOnca Data Solutions.

def map_aluno_to_dict(row):
    """Mapeia uma linha da tabela 'aluno' para um dicionário."""
    return {
        "ALU_ATIVO": row.ALU_ATIVO,
        "ALU_AVATAR": row.ALU_AVATAR,
        "ALU_BAIRRO": row.ALU_BAIRRO,
        "ALU_CEP": row.ALU_CEP,
        "ALU_CIDADE": row.ALU_CIDADE,
        "ALU_COD": row.ALU_COD,
        "ALU_COMPLEMENTO": row.ALU_COMPLEMENTO,
        "ALU_CPF": row.ALU_CPF,
        "ALU_DEFICIENCIA_BY_IMPORT": row.ALU_DEFICIENCIA_BY_IMPORT,
        "ALU_DT_ATUALIZACAO": row.ALU_DT_ATUALIZACAO,
        "ALU_DT_CRIACAO": row.ALU_DT_CRIACAO,
        "ALU_DT_NASC": row.ALU_DT_NASC,
        "ALU_EMAIL": row.ALU_EMAIL,
        "ALU_ENDERECO": row.ALU_ENDERECO,
        "ALU_ESC_ID": row.ALU_ESC_ID,
        "ALU_GEN_ID": row.ALU_GEN_ID,
        "ALU_ID": row.ALU_ID,
        "ALU_INEP": row.ALU_INEP,
        "ALU_NOME": row.ALU_NOME,
        "ALU_NOME_MAE": row.ALU_NOME_MAE,
        "ALU_NOME_PAI": row.ALU_NOME_PAI,
        "ALU_NOME_RESP": row.ALU_NOME_RESP,
        "ALU_NUMERO": row.ALU_NUMERO,
        "ALU_OLD_ID": row.ALU_OLD_ID,
        "ALU_PCD_ID": row.ALU_PCD_ID,
        "ALU_PEL_ID": row.ALU_PEL_ID,
        "ALU_SER_ID": row.ALU_SER_ID,
        "ALU_STATUS": row.ALU_STATUS,
        "ALU_TEL1": row.ALU_TEL1,
        "ALU_TEL2": row.ALU_TEL2,
        "ALU_TUR_ID": row.ALU_TUR_ID,
        "ALU_UF": row.ALU_UF,
        "ALU_WHATSAPP": row.ALU_WHATSAPP,
        }
def map_aluno_alu_deficiencias_pcd_to_dict(row):
    """Mapeia uma linha da tabela 'aluno_alu_deficiencias_pcd' para um dicionário."""
    return {
        "alunoALUID": row.alunoALUID,
        "pcdPCDID": row.pcdPCDID,
        }
def map_aluno_teste_to_dict(row):
    """Mapeia uma linha da tabela 'aluno_teste' para um dicionário."""
    return {
        "ALT_ALU_ID": row.ALT_ALU_ID,
        "ALT_ATIVO": row.ALT_ATIVO,
        "ALT_BY_AVA_ONLINE": row.ALT_BY_AVA_ONLINE,
        "ALT_BY_EDLER": row.ALT_BY_EDLER,
        "ALT_BY_HERBY": row.ALT_BY_HERBY,
        "ALT_DT_ATUALIZACAO": row.ALT_DT_ATUALIZACAO,
        "ALT_DT_CRIACAO": row.ALT_DT_CRIACAO,
        "ALT_FINALIZADO": row.ALT_FINALIZADO,
        "ALT_FORNECEDOR": row.ALT_FORNECEDOR,
        "ALT_ID": row.ALT_ID,
        "ALT_JUSTIFICATIVA": row.ALT_JUSTIFICATIVA,
        "ALT_TES_ID": row.ALT_TES_ID,
        "ALT_USU_ID": row.ALT_USU_ID,
        "schoolClassTURID": row.schoolClassTURID,
        }
def map_aluno_teste_resposta_to_dict(row):
    """Mapeia uma linha da tabela 'aluno_teste_resposta' para um dicionário."""
    return {
        "ATR_ALT_ID": row.ATR_ALT_ID,
        "ATR_CERTO": row.ATR_CERTO,
        "ATR_DT_CRIACAO": row.ATR_DT_CRIACAO,
        "ATR_ID": row.ATR_ID,
        "ATR_MTI_ID": row.ATR_MTI_ID,
        "ATR_RESPOSTA": row.ATR_RESPOSTA,
        "questionTemplateTEGID": row.questionTemplateTEGID,
        }
def map_aluno_teste_resposta_historico_to_dict(row):
    """Mapeia uma linha da tabela 'aluno_teste_resposta_historico' para um dicionário."""
    return {
        "ATH_ALT_ID": row.ATH_ALT_ID,
        "ATH_ATR_ID": row.ATH_ATR_ID,
        "ATH_ATR_RESPOSTA_ANTIGA": row.ATH_ATR_RESPOSTA_ANTIGA,
        "ATH_ATR_RESPOSTA_NOVA": row.ATH_ATR_RESPOSTA_NOVA,
        "ATH_DT_CRIACAO": row.ATH_DT_CRIACAO,
        "ATH_ID": row.ATH_ID,
        "ATH_OPERACAO": row.ATH_OPERACAO,
        "ATH_TEG_ID": row.ATH_TEG_ID,
        }
def map_ano_letivo_to_dict(row):
    """Mapeia uma linha da tabela 'ano_letivo' para um dicionário."""
    return {
        "ANO_ATIVO": row.ANO_ATIVO,
        "ANO_DT_ATUALIZACAO": row.ANO_DT_ATUALIZACAO,
        "ANO_DT_CRIACAO": row.ANO_DT_CRIACAO,
        "ANO_ID": row.ANO_ID,
        "ANO_NOME": row.ANO_NOME,
        "ANO_OLD_ID": row.ANO_OLD_ID,
        }
def map_area_to_dict(row):
    """Mapeia uma linha da tabela 'area' para um dicionário."""
    return {
        "ARE_ATIVO": row.ARE_ATIVO,
        "ARE_DESCRICAO": row.ARE_DESCRICAO,
        "ARE_DT_ATUALIZACAO": row.ARE_DT_ATUALIZACAO,
        "ARE_DT_CRIACAO": row.ARE_DT_CRIACAO,
        "ARE_ID": row.ARE_ID,
        "ARE_NOME": row.ARE_NOME,
        }
def map_arquivo_to_dict(row):
    """Mapeia uma linha da tabela 'arquivo' para um dicionário."""
    return {
        "ARQ_ATIVO": row.ARQ_ATIVO,
        "ARQ_DT_ATUALIZACAO": row.ARQ_DT_ATUALIZACAO,
        "ARQ_DT_CRIACAO": row.ARQ_DT_CRIACAO,
        "ARQ_ID": row.ARQ_ID,
        "ARQ_NOME": row.ARQ_NOME,
        "ARQ_URL": row.ARQ_URL,
        }
def map_avaliacao_to_dict(row):
    """Mapeia uma linha da tabela 'avaliacao' para um dicionário."""
    return {
        "AVA_ANO": row.AVA_ANO,
        "AVA_ATIVO": row.AVA_ATIVO,
        "AVA_DT_ATUALIZACAO": row.AVA_DT_ATUALIZACAO,
        "AVA_DT_CRIACAO": row.AVA_DT_CRIACAO,
        "AVA_ID": row.AVA_ID,
        "AVA_NOME": row.AVA_NOME,
        "AVA_OLD_ID": row.AVA_OLD_ID,
        }
def map_avaliacao_municipio_to_dict(row):
    """Mapeia uma linha da tabela 'avaliacao_municipio' para um dicionário."""
    return {
        "AVM_ATIVO": row.AVM_ATIVO,
        "AVM_AVA_ID": row.AVM_AVA_ID,
        "AVM_DT_ATUALIZACAO": row.AVM_DT_ATUALIZACAO,
        "AVM_DT_CRIACAO": row.AVM_DT_CRIACAO,
        "AVM_DT_DISPONIVEL": row.AVM_DT_DISPONIVEL,
        "AVM_DT_FIM": row.AVM_DT_FIM,
        "AVM_DT_INICIO": row.AVM_DT_INICIO,
        "AVM_ID": row.AVM_ID,
        "AVM_MUN_ID": row.AVM_MUN_ID,
        "AVM_OLD_ID": row.AVM_OLD_ID,
        "AVM_TIPO": row.AVM_TIPO,
        }
def map_avaliacao_online_to_dict(row):
    """Mapeia uma linha da tabela 'avaliacao_online' para um dicionário."""
    return {
        "active": row.active,
        "createdAt": row.createdAt,
        "id": row.id,
        "updatedAt": row.updatedAt,
        }
def map_avaliacao_online_page_to_dict(row):
    """Mapeia uma linha da tabela 'avaliacao_online_page' para um dicionário."""
    return {
        "assessmentOnlineId": row.assessmentOnlineId,
        "createdAt": row.createdAt,
        "id": row.id,
        "image": row.image,
        "order": row.order,
        "title": row.title,
        "updatedAt": row.updatedAt,
        }
def map_avaliacao_online_question_to_dict(row):
    """Mapeia uma linha da tabela 'avaliacao_online_question' para um dicionário."""
    return {
        "createdAt": row.createdAt,
        "description": row.description,
        "id": row.id,
        "order": row.order,
        "pageId": row.pageId,
        "questionTemplateId": row.questionTemplateId,
        "updatedAt": row.updatedAt,
        }
def map_avaliacao_online_question_alternative_to_dict(row):
    """Mapeia uma linha da tabela 'avaliacao_online_question_alternative' para um dicionário."""
    return {
        "createdAt": row.createdAt,
        "description": row.description,
        "id": row.id,
        "image": row.image,
        "option": row.option,
        "questionId": row.questionId,
        "updatedAt": row.updatedAt,
        }
def map_avaliacao_teste_to_dict(row):
    """Mapeia uma linha da tabela 'avaliacao_teste' para um dicionário."""
    return {
        "AVA_ID": row.AVA_ID,
        "TES_ID": row.TES_ID,
        }
def map_dados_to_dict(row):
    """Mapeia uma linha da tabela 'dados' para um dicionário."""
    return {
        "DAT_ARQUIVO": row.DAT_ARQUIVO,
        "DAT_DT_ATUALIZACAO": row.DAT_DT_ATUALIZACAO,
        "DAT_DT_CRIACAO": row.DAT_DT_CRIACAO,
        "DAT_ID": row.DAT_ID,
        "DAT_NOME": row.DAT_NOME,
        "DAT_OBS": row.DAT_OBS,
        "DAT_STATUS": row.DAT_STATUS,
        "DAT_TIPO": row.DAT_TIPO,
        "DAT_USU_ID": row.DAT_USU_ID,
        }
def map_disciplina_to_dict(row):
    """Mapeia uma linha da tabela 'disciplina' para um dicionário."""
    return {
        "DIS_ATIVO": row.DIS_ATIVO,
        "DIS_COLOR": row.DIS_COLOR,
        "DIS_DT_ATUALIZACAO": row.DIS_DT_ATUALIZACAO,
        "DIS_DT_CRIACAO": row.DIS_DT_CRIACAO,
        "DIS_ID": row.DIS_ID,
        "DIS_NOME": row.DIS_NOME,
        "DIS_OLD_ID": row.DIS_OLD_ID,
        "DIS_TIPO": row.DIS_TIPO,
        }
def map_envios_tutor_mensagens_to_dict(row):
    """Mapeia uma linha da tabela 'envios_tutor_mensagens' para um dicionário."""
    return {
        "createdAt": row.createdAt,
        "id": row.id,
        "statusEmail": row.statusEmail,
        "statusWhatsapp": row.statusWhatsapp,
        "studentId": row.studentId,
        "tutorMessageId": row.tutorMessageId,
        }
def map_escola_to_dict(row):
    """Mapeia uma linha da tabela 'escola' para um dicionário."""
    return {
        "ESC_ATIVO": row.ESC_ATIVO,
        "ESC_BAIRRO": row.ESC_BAIRRO,
        "ESC_CEP": row.ESC_CEP,
        "ESC_CIDADE": row.ESC_CIDADE,
        "ESC_COMPLEMENTO": row.ESC_COMPLEMENTO,
        "ESC_DT_ATUALIZACAO": row.ESC_DT_ATUALIZACAO,
        "ESC_DT_CRIACAO": row.ESC_DT_CRIACAO,
        "ESC_ENDERECO": row.ESC_ENDERECO,
        "ESC_ID": row.ESC_ID,
        "ESC_INEP": row.ESC_INEP,
        "ESC_INTEGRAL": row.ESC_INTEGRAL,
        "ESC_LOGO": row.ESC_LOGO,
        "ESC_MUN_ID": row.ESC_MUN_ID,
        "ESC_NOME": row.ESC_NOME,
        "ESC_NUMERO": row.ESC_NUMERO,
        "ESC_OLD_ID": row.ESC_OLD_ID,
        "ESC_STATUS": row.ESC_STATUS,
        "ESC_TIPO": row.ESC_TIPO,
        "ESC_UF": row.ESC_UF,
        "regionalId": row.regionalId,
        }
def map_estados_to_dict(row):
    """Mapeia uma linha da tabela 'estados' para um dicionário."""
    return {
        "abbreviation": row.abbreviation,
        "active": row.active,
        "createdAt": row.createdAt,
        "id": row.id,
        "name": row.name,
        "updatedAt": row.updatedAt,
        }
def map_external_reports_to_dict(row):
    """Mapeia uma linha da tabela 'external_reports' para um dicionário."""
    return {
        "active": row.active,
        "category": row.category,
        "createdAt": row.createdAt,
        "description": row.description,
        "id": row.id,
        "link": row.link,
        "name": row.name,
        "role": row.role,
        "updatedAt": row.updatedAt,
        }
def map_forget_password_to_dict(row):
    """Mapeia uma linha da tabela 'forget_password' para um dicionário."""
    return {
        "createdAt": row.createdAt,
        "id": row.id,
        "isValid": row.isValid,
        "token": row.token,
        "updatedAt": row.updatedAt,
        "userUSUID": row.userUSUID,
        }
def map_formacao_to_dict(row):
    """Mapeia uma linha da tabela 'formacao' para um dicionário."""
    return {
        "FOR_ATIVO": row.FOR_ATIVO,
        "FOR_DT_ATUALIZACAO": row.FOR_DT_ATUALIZACAO,
        "FOR_DT_CRIACAO": row.FOR_DT_CRIACAO,
        "FOR_ID": row.FOR_ID,
        "FOR_NOME": row.FOR_NOME,
        }
def map_genero_to_dict(row):
    """Mapeia uma linha da tabela 'genero' para um dicionário."""
    return {
        "GEN_ATIVO": row.GEN_ATIVO,
        "GEN_DT_ATUALIZACAO": row.GEN_DT_ATUALIZACAO,
        "GEN_DT_CRIACAO": row.GEN_DT_CRIACAO,
        "GEN_ID": row.GEN_ID,
        "GEN_NOME": row.GEN_NOME,
        }
def map_importar_dados_to_dict(row):
    """Mapeia uma linha da tabela 'importar_dados' para um dicionário."""
    return {
        "DAT_ARQUIVO_ERROR_URL": row.DAT_ARQUIVO_ERROR_URL,
        "DAT_ARQUIVO_URL": row.DAT_ARQUIVO_URL,
        "DAT_DT_ATUALIZACAO": row.DAT_DT_ATUALIZACAO,
        "DAT_DT_CRIACAO": row.DAT_DT_CRIACAO,
        "DAT_ID": row.DAT_ID,
        "DAT_NOME": row.DAT_NOME,
        "DAT_OBS": row.DAT_OBS,
        "DAT_STATUS": row.DAT_STATUS,
        "dATUSUUSUID": row.dATUSUUSUID,
        }
def map_infrequencia_to_dict(row):
    """Mapeia uma linha da tabela 'infrequencia' para um dicionário."""
    return {
        "IFR_ALU_ID": row.IFR_ALU_ID,
        "IFR_ANO": row.IFR_ANO,
        "IFR_DT_ATUALIZACAO": row.IFR_DT_ATUALIZACAO,
        "IFR_DT_CRIACAO": row.IFR_DT_CRIACAO,
        "IFR_FALTA": row.IFR_FALTA,
        "IFR_ID": row.IFR_ID,
        "IFR_MES": row.IFR_MES,
        "IFR_OLD_ID": row.IFR_OLD_ID,
        "IFR_SCHOOL_CLASS_ID": row.IFR_SCHOOL_CLASS_ID,
        }
def map_job_to_dict(row):
    """Mapeia uma linha da tabela 'job' para um dicionário."""
    return {
        "assessmentId": row.assessmentId,
        "bullId": row.bullId,
        "countyId": row.countyId,
        "createdAt": row.createdAt,
        "endDate": row.endDate,
        "id": row.id,
        "jobType": row.jobType,
        "startDate": row.startDate,
        "updatedAt": row.updatedAt,
        }
def map_matriz_referencia_to_dict(row):
    """Mapeia uma linha da tabela 'matriz_referencia' para um dicionário."""
    return {
        "MAR_ATIVO": row.MAR_ATIVO,
        "MAR_DIS_ID": row.MAR_DIS_ID,
        "MAR_DT_ATUALIZACAO": row.MAR_DT_ATUALIZACAO,
        "MAR_DT_CRIACAO": row.MAR_DT_CRIACAO,
        "MAR_ID": row.MAR_ID,
        "MAR_NOME": row.MAR_NOME,
        "MAR_OLD_ID": row.MAR_OLD_ID,
        }
def map_matriz_referencia_serie_to_dict(row):
    """Mapeia uma linha da tabela 'matriz_referencia_serie' para um dicionário."""
    return {
        "MAR_ID": row.MAR_ID,
        "SER_ID": row.SER_ID,
        }
def map_matriz_referencia_topico_to_dict(row):
    """Mapeia uma linha da tabela 'matriz_referencia_topico' para um dicionário."""
    return {
        "MTO_ATIVO": row.MTO_ATIVO,
        "MTO_DT_ATUALIZACAO": row.MTO_DT_ATUALIZACAO,
        "MTO_DT_CRIACAO": row.MTO_DT_CRIACAO,
        "MTO_ID": row.MTO_ID,
        "MTO_MAR_ID": row.MTO_MAR_ID,
        "MTO_NOME": row.MTO_NOME,
        "MTO_OLD_ID": row.MTO_OLD_ID,
        }
def map_matriz_referencia_topico_items_to_dict(row):
    """Mapeia uma linha da tabela 'matriz_referencia_topico_items' para um dicionário."""
    return {
        "MTI_ATIVO": row.MTI_ATIVO,
        "MTI_CODIGO": row.MTI_CODIGO,
        "MTI_DESCRITOR": row.MTI_DESCRITOR,
        "MTI_DT_ATUALIZACAO": row.MTI_DT_ATUALIZACAO,
        "MTI_DT_CRIACAO": row.MTI_DT_CRIACAO,
        "MTI_ID": row.MTI_ID,
        "MTI_MTO_ID": row.MTI_MTO_ID,
        "MTI_OLD_ID": row.MTI_OLD_ID,
        }
def map_messages_to_dict(row):
    """Mapeia uma linha da tabela 'messages' para um dicionário."""
    return {
        "MEN_DT_ATUALIZACAO": row.MEN_DT_ATUALIZACAO,
        "MEN_DT_CRIACAO": row.MEN_DT_CRIACAO,
        "MEN_ID": row.MEN_ID,
        "MEN_IS_DELETE": row.MEN_IS_DELETE,
        "MEN_TEXT": row.MEN_TEXT,
        "MEN_TITLE": row.MEN_TITLE,
        }
def map_messages_municipios_municipio_to_dict(row):
    """Mapeia uma linha da tabela 'messages_municipios_municipio' para um dicionário."""
    return {
        "messagesMENID": row.messagesMENID,
        "municipioMUNID": row.municipioMUNID,
        }
def map_messages_schools_escola_to_dict(row):
    """Mapeia uma linha da tabela 'messages_schools_escola' para um dicionário."""
    return {
        "escolaESCID": row.escolaESCID,
        "messagesMENID": row.messagesMENID,
        }
def map_microdata_to_dict(row):
    """Mapeia uma linha da tabela 'microdata' para um dicionário."""
    return {
        "countyMUNID": row.countyMUNID,
        "createdAt": row.createdAt,
        "file": row.file,
        "id": row.id,
        "stateId": row.stateId,
        "status": row.status,
        "type": row.type,
        "typeSchool": row.typeSchool,
        "updatedAt": row.updatedAt,
        "userUSUID": row.userUSUID,
        }
def map_migrations_to_dict(row):
    """Mapeia uma linha da tabela 'migrations' para um dicionário."""
    return {
        "id": row.id,
        "name": row.name,
        "timestamp": row.timestamp,
        }
def map_municipio_to_dict(row):
    """Mapeia uma linha da tabela 'municipio' para um dicionário."""
    return {
        "MUN_ARQ_CONVENIO": row.MUN_ARQ_CONVENIO,
        "MUN_ATIVO": row.MUN_ATIVO,
        "MUN_BAIRRO": row.MUN_BAIRRO,
        "MUN_CEP": row.MUN_CEP,
        "MUN_CIDADE": row.MUN_CIDADE,
        "MUN_COD_IBGE": row.MUN_COD_IBGE,
        "MUN_COMPARTILHAR_DADOS": row.MUN_COMPARTILHAR_DADOS,
        "MUN_COMPLEMENTO": row.MUN_COMPLEMENTO,
        "MUN_DT_ATUALIZACAO": row.MUN_DT_ATUALIZACAO,
        "MUN_DT_CRIACAO": row.MUN_DT_CRIACAO,
        "MUN_DT_FIM": row.MUN_DT_FIM,
        "MUN_DT_INICIO": row.MUN_DT_INICIO,
        "MUN_ENDERECO": row.MUN_ENDERECO,
        "MUN_ID": row.MUN_ID,
        "MUN_LOGO": row.MUN_LOGO,
        "MUN_MENSAGEM_EMAIL_ATIVO": row.MUN_MENSAGEM_EMAIL_ATIVO,
        "MUN_MENSAGEM_WHATSAPP_ATIVO": row.MUN_MENSAGEM_WHATSAPP_ATIVO,
        "MUN_NOME": row.MUN_NOME,
        "MUN_NUMERO": row.MUN_NUMERO,
        "MUN_OLD_ID": row.MUN_OLD_ID,
        "MUN_PARCEIRO_EPV": row.MUN_PARCEIRO_EPV,
        "MUN_STATUS": row.MUN_STATUS,
        "MUN_UF": row.MUN_UF,
        "stateId": row.stateId,
        "stateRegionalId": row.stateRegionalId,
        }
def map_notifications_to_dict(row):
    """Mapeia uma linha da tabela 'notifications' para um dicionário."""
    return {
        "createdAt": row.createdAt,
        "id": row.id,
        "isReading": row.isReading,
        "message": row.message,
        "title": row.title,
        "updateAt": row.updateAt,
        "userUSUID": row.userUSUID,
        }
def map_pcd_to_dict(row):
    """Mapeia uma linha da tabela 'pcd' para um dicionário."""
    return {
        "PCD_ATIVO": row.PCD_ATIVO,
        "PCD_DT_ATUALIZACAO": row.PCD_DT_ATUALIZACAO,
        "PCD_DT_CRIACAO": row.PCD_DT_CRIACAO,
        "PCD_ID": row.PCD_ID,
        "PCD_NOME": row.PCD_NOME,
        "PCD_OLD_ID": row.PCD_OLD_ID,
        }
def map_perfil_base_to_dict(row):
    """Mapeia uma linha da tabela 'perfil_base' para um dicionário."""
    return {
        "PER_ATIVO": row.PER_ATIVO,
        "PER_DT_ATUALIZACAO": row.PER_DT_ATUALIZACAO,
        "PER_DT_CRIACAO": row.PER_DT_CRIACAO,
        "PER_ID": row.PER_ID,
        "PER_NOME": row.PER_NOME,
        }
def map_professor_to_dict(row):
    """Mapeia uma linha da tabela 'professor' para um dicionário."""
    return {
        "PRO_ATIVO": row.PRO_ATIVO,
        "PRO_AVATAR": row.PRO_AVATAR,
        "PRO_BAIRRO": row.PRO_BAIRRO,
        "PRO_CEP": row.PRO_CEP,
        "PRO_CIDADE": row.PRO_CIDADE,
        "PRO_COMPLEMENTO": row.PRO_COMPLEMENTO,
        "PRO_DOCUMENTO": row.PRO_DOCUMENTO,
        "PRO_DT_ATUALIZACAO": row.PRO_DT_ATUALIZACAO,
        "PRO_DT_CRIACAO": row.PRO_DT_CRIACAO,
        "PRO_DT_NASC": row.PRO_DT_NASC,
        "PRO_EMAIL": row.PRO_EMAIL,
        "PRO_ENDERECO": row.PRO_ENDERECO,
        "PRO_FONE": row.PRO_FONE,
        "PRO_FOR_ID": row.PRO_FOR_ID,
        "PRO_GEN_ID": row.PRO_GEN_ID,
        "PRO_ID": row.PRO_ID,
        "PRO_MUN_ID": row.PRO_MUN_ID,
        "PRO_NOME": row.PRO_NOME,
        "PRO_NUMERO": row.PRO_NUMERO,
        "PRO_OLD_ID": row.PRO_OLD_ID,
        "PRO_PEL_ID": row.PRO_PEL_ID,
        "PRO_UF": row.PRO_UF,
        }
def map_raca_to_dict(row):
    """Mapeia uma linha da tabela 'raca' para um dicionário."""
    return {
        "PEL_ATIVO": row.PEL_ATIVO,
        "PEL_DT_ATUALIZACAO": row.PEL_DT_ATUALIZACAO,
        "PEL_DT_CRIACAO": row.PEL_DT_CRIACAO,
        "PEL_ID": row.PEL_ID,
        "PEL_NOME": row.PEL_NOME,
        "PEL_OLD_ID": row.PEL_OLD_ID,
        }
def map_regionais_to_dict(row):
    """Mapeia uma linha da tabela 'regionais' para um dicionário."""
    return {
        "active": row.active,
        "countyId": row.countyId,
        "createdAt": row.createdAt,
        "id": row.id,
        "name": row.name,
        "stateId": row.stateId,
        "type": row.type,
        "updatedAt": row.updatedAt,
        }
def map_report_descriptor_to_dict(row):
    """Mapeia uma linha da tabela 'report_descriptor' para um dicionário."""
    return {
        "createdAt": row.createdAt,
        "descriptorMTIID": row.descriptorMTIID,
        "id": row.id,
        "reportEditionId": row.reportEditionId,
        "testTESID": row.testTESID,
        "total": row.total,
        "totalCorrect": row.totalCorrect,
        "updatedAt": row.updatedAt,
        }
def map_report_edition_to_dict(row):
    """Mapeia uma linha da tabela 'report_edition' para um dicionário."""
    return {
        "countyMUNID": row.countyMUNID,
        "createdAt": row.createdAt,
        "editionAVAID": row.editionAVAID,
        "id": row.id,
        "regionalId": row.regionalId,
        "schoolClassTURID": row.schoolClassTURID,
        "schoolESCID": row.schoolESCID,
        "type": row.type,
        "updatedAt": row.updatedAt,
        }
def map_report_not_evaluated_to_dict(row):
    """Mapeia uma linha da tabela 'report_not_evaluated' para um dicionário."""
    return {
        "abandono": row.abandono,
        "ausencia": row.ausencia,
        "countPresentStudents": row.countPresentStudents,
        "countStudentsLaunched": row.countStudentsLaunched,
        "countTotalStudents": row.countTotalStudents,
        "createdAt": row.createdAt,
        "deficiencia": row.deficiencia,
        "id": row.id,
        "idStudents": row.idStudents,
        "name": row.name,
        "nao_participou": row.nao_participou,
        "recusa": row.recusa,
        "reportEditionId": row.reportEditionId,
        "testTESID": row.testTESID,
        "transferencia": row.transferencia,
        "type": row.type,
        "updatedAt": row.updatedAt,
        }
def map_report_question_to_dict(row):
    """Mapeia uma linha da tabela 'report_question' para um dicionário."""
    return {
        "fluente": row.fluente,
        "frases": row.frases,
        "id": row.id,
        "nao_avaliado": row.nao_avaliado,
        "nao_fluente": row.nao_fluente,
        "nao_informado": row.nao_informado,
        "nao_leitor": row.nao_leitor,
        "option_correct": row.option_correct,
        "palavras": row.palavras,
        "questionTEGID": row.questionTEGID,
        "reportSubjectId": row.reportSubjectId,
        "silabas": row.silabas,
        "total_a": row.total_a,
        "total_b": row.total_b,
        "total_c": row.total_c,
        "total_d": row.total_d,
        "total_null": row.total_null,
        }
def map_report_question_option_to_dict(row):
    """Mapeia uma linha da tabela 'report_question_option' para um dicionário."""
    return {
        "createdAt": row.createdAt,
        "fluente": row.fluente,
        "frases": row.frases,
        "id": row.id,
        "nao_avaliado": row.nao_avaliado,
        "nao_fluente": row.nao_fluente,
        "nao_informado": row.nao_informado,
        "nao_leitor": row.nao_leitor,
        "option": row.option,
        "palavras": row.palavras,
        "reportQuestionId": row.reportQuestionId,
        "silabas": row.silabas,
        "totalCorrect": row.totalCorrect,
        }
def map_report_race_to_dict(row):
    """Mapeia uma linha da tabela 'report_race' para um dicionário."""
    return {
        "countPresentStudents": row.countPresentStudents,
        "countStudentsLaunched": row.countStudentsLaunched,
        "countTotalStudents": row.countTotalStudents,
        "createdAt": row.createdAt,
        "fluente": row.fluente,
        "frases": row.frases,
        "id": row.id,
        "idStudents": row.idStudents,
        "name": row.name,
        "nao_avaliado": row.nao_avaliado,
        "nao_fluente": row.nao_fluente,
        "nao_informado": row.nao_informado,
        "nao_leitor": row.nao_leitor,
        "palavras": row.palavras,
        "racePELID": row.racePELID,
        "reportSubjectId": row.reportSubjectId,
        "silabas": row.silabas,
        "totalGradesStudents": row.totalGradesStudents,
        "updatedAt": row.updatedAt,
        }
def map_report_subject_to_dict(row):
    """Mapeia uma linha da tabela 'report_subject' para um dicionário."""
    return {
        "countPresentStudents": row.countPresentStudents,
        "countStudentsLaunched": row.countStudentsLaunched,
        "countTotalStudents": row.countTotalStudents,
        "createdAt": row.createdAt,
        "fluente": row.fluente,
        "frases": row.frases,
        "id": row.id,
        "idStudents": row.idStudents,
        "name": row.name,
        "nao_avaliado": row.nao_avaliado,
        "nao_fluente": row.nao_fluente,
        "nao_informado": row.nao_informado,
        "nao_leitor": row.nao_leitor,
        "palavras": row.palavras,
        "reportEditionId": row.reportEditionId,
        "silabas": row.silabas,
        "testTESID": row.testTESID,
        "totalGradesStudents": row.totalGradesStudents,
        "type": row.type,
        "updatedAt": row.updatedAt,
        }
def map_series_to_dict(row):
    """Mapeia uma linha da tabela 'series' para um dicionário."""
    return {
        "SER_ATIVO": row.SER_ATIVO,
        "SER_DT_ATUALIZACAO": row.SER_DT_ATUALIZACAO,
        "SER_DT_CRIACAO": row.SER_DT_CRIACAO,
        "SER_ID": row.SER_ID,
        "SER_NOME": row.SER_NOME,
        "SER_NUMBER": row.SER_NUMBER,
        "SER_OLD_ID": row.SER_OLD_ID,
        }
def map_sub_perfil_to_dict(row):
    """Mapeia uma linha da tabela 'sub_perfil' para um dicionário."""
    return {
        "SPE_ATIVO": row.SPE_ATIVO,
        "SPE_DT_ATUALIZACAO": row.SPE_DT_ATUALIZACAO,
        "SPE_DT_CRIACAO": row.SPE_DT_CRIACAO,
        "SPE_ID": row.SPE_ID,
        "SPE_NOME": row.SPE_NOME,
        "SPE_PER_ID": row.SPE_PER_ID,
        "role": row.role,
        }
def map_sub_perfil_area_to_dict(row):
    """Mapeia uma linha da tabela 'sub_perfil_area' para um dicionário."""
    return {
        "ARE_ID": row.ARE_ID,
        "SPE_ID": row.SPE_ID,
        }
def map_system_logs_to_dict(row):
    """Mapeia uma linha da tabela 'system_logs' para um dicionário."""
    return {
        "createdAt": row.createdAt,
        "id": row.id,
        "method": row.method,
        "nameEntity": row.nameEntity,
        "stateFinal": row.stateFinal,
        "stateInitial": row.stateInitial,
        "updatedAt": row.updatedAt,
        "userUSUID": row.userUSUID,
        }
def map_templates_mensagens_to_dict(row):
    """Mapeia uma linha da tabela 'templates_mensagens' para um dicionário."""
    return {
        "content": row.content,
        "createdAt": row.createdAt,
        "id": row.id,
        "schoolId": row.schoolId,
        "title": row.title,
        "updatedAt": row.updatedAt,
        }
def map_teste_to_dict(row):
    """Mapeia uma linha da tabela 'teste' para um dicionário."""
    return {
        "TES_ANO": row.TES_ANO,
        "TES_ARQUIVO": row.TES_ARQUIVO,
        "TES_ATIVO": row.TES_ATIVO,
        "TES_DIS_ID": row.TES_DIS_ID,
        "TES_DT_ATUALIZACAO": row.TES_DT_ATUALIZACAO,
        "TES_DT_CRIACAO": row.TES_DT_CRIACAO,
        "TES_ID": row.TES_ID,
        "TES_MANUAL": row.TES_MANUAL,
        "TES_MAR_ID": row.TES_MAR_ID,
        "TES_NOME": row.TES_NOME,
        "TES_OLD_ID": row.TES_OLD_ID,
        "TES_SER_ID": row.TES_SER_ID,
        "assessmentOnlineId": row.assessmentOnlineId,
        }
def map_teste_gabarito_to_dict(row):
    """Mapeia uma linha da tabela 'teste_gabarito' para um dicionário."""
    return {
        "TEG_DT_ATUALIZACAO": row.TEG_DT_ATUALIZACAO,
        "TEG_DT_CRIACAO": row.TEG_DT_CRIACAO,
        "TEG_ID": row.TEG_ID,
        "TEG_MTI_ID": row.TEG_MTI_ID,
        "TEG_OLD_ID": row.TEG_OLD_ID,
        "TEG_ORDEM": row.TEG_ORDEM,
        "TEG_RESPOSTA_CORRETA": row.TEG_RESPOSTA_CORRETA,
        "TEG_TES_ID": row.TEG_TES_ID,
        }
def map_transferencia_to_dict(row):
    """Mapeia uma linha da tabela 'transferencia' para um dicionário."""
    return {
        "TRF_ALU_ID": row.TRF_ALU_ID,
        "TRF_DT_ATUALIZACAO": row.TRF_DT_ATUALIZACAO,
        "TRF_DT_CRIACAO": row.TRF_DT_CRIACAO,
        "TRF_ESC_ID_DESTINO": row.TRF_ESC_ID_DESTINO,
        "TRF_ESC_ID_ORIGEM": row.TRF_ESC_ID_ORIGEM,
        "TRF_ID": row.TRF_ID,
        "TRF_JUSTIFICATIVA": row.TRF_JUSTIFICATIVA,
        "TRF_OLD_ID": row.TRF_OLD_ID,
        "TRF_STATUS": row.TRF_STATUS,
        "TRF_TUR_ID_DESTINO": row.TRF_TUR_ID_DESTINO,
        "TRF_TUR_ID_ORIGEM": row.TRF_TUR_ID_ORIGEM,
        "TRF_USU": row.TRF_USU,
        "TRF_USU_STATUS": row.TRF_USU_STATUS,
        }
def map_turma_to_dict(row):
    """Mapeia uma linha da tabela 'turma' para um dicionário."""
    return {
        "TUR_ANEXO": row.TUR_ANEXO,
        "TUR_ANO": row.TUR_ANO,
        "TUR_ATIVO": row.TUR_ATIVO,
        "TUR_DT_ATUALIZACAO": row.TUR_DT_ATUALIZACAO,
        "TUR_DT_CRIACAO": row.TUR_DT_CRIACAO,
        "TUR_ESC_ID": row.TUR_ESC_ID,
        "TUR_ID": row.TUR_ID,
        "TUR_MUN_ID": row.TUR_MUN_ID,
        "TUR_NOME": row.TUR_NOME,
        "TUR_OLD_ID": row.TUR_OLD_ID,
        "TUR_PERIODO": row.TUR_PERIODO,
        "TUR_SER_ID": row.TUR_SER_ID,
        "TUR_TIPO": row.TUR_TIPO,
        }
def map_turma_aluno_to_dict(row):
    """Mapeia uma linha da tabela 'turma_aluno' para um dicionário."""
    return {
        "createdAt": row.createdAt,
        "endDate": row.endDate,
        "id": row.id,
        "schoolClassTURID": row.schoolClassTURID,
        "startDate": row.startDate,
        "studentALUID": row.studentALUID,
        "updatedAt": row.updatedAt,
        }
def map_turma_professor_to_dict(row):
    """Mapeia uma linha da tabela 'turma_professor' para um dicionário."""
    return {
        "PRO_ID": row.PRO_ID,
        "TUR_ID": row.TUR_ID,
        }
def map_tutor_mensagens_to_dict(row):
    """Mapeia uma linha da tabela 'tutor_mensagens' para um dicionário."""
    return {
        "content": row.content,
        "createdAt": row.createdAt,
        "filters": row.filters,
        "id": row.id,
        "schoolId": row.schoolId,
        "title": row.title,
        "updatedAt": row.updatedAt,
        }
def map_usuario_to_dict(row):
    """Mapeia uma linha da tabela 'usuario' para um dicionário."""
    return {
        "USU_ATIVO": row.USU_ATIVO,
        "USU_AVATAR": row.USU_AVATAR,
        "USU_DOCUMENTO": row.USU_DOCUMENTO,
        "USU_DT_ATUALIZACAO": row.USU_DT_ATUALIZACAO,
        "USU_DT_CRIACAO": row.USU_DT_CRIACAO,
        "USU_EMAIL": row.USU_EMAIL,
        "USU_ESC_ID": row.USU_ESC_ID,
        "USU_FONE": row.USU_FONE,
        "USU_ID": row.USU_ID,
        "USU_MUN_ID": row.USU_MUN_ID,
        "USU_NOME": row.USU_NOME,
        "USU_SENHA": row.USU_SENHA,
        "USU_SPE_ID": row.USU_SPE_ID,
        "isChangePasswordWelcome": row.isChangePasswordWelcome,
        "stateId": row.stateId,
        }
MAP_FUNCTIONS = {
    "map_aluno_to_dict": map_aluno_to_dict,
    "map_aluno_alu_deficiencias_pcd_to_dict": map_aluno_alu_deficiencias_pcd_to_dict,
    "map_aluno_teste_to_dict": map_aluno_teste_to_dict,
    "map_aluno_teste_resposta_to_dict": map_aluno_teste_resposta_to_dict,
    "map_aluno_teste_resposta_historico_to_dict": map_aluno_teste_resposta_historico_to_dict,
    "map_ano_letivo_to_dict": map_ano_letivo_to_dict,
    "map_area_to_dict": map_area_to_dict,
    "map_arquivo_to_dict": map_arquivo_to_dict,
    "map_avaliacao_to_dict": map_avaliacao_to_dict,
    "map_avaliacao_municipio_to_dict": map_avaliacao_municipio_to_dict,
    "map_avaliacao_online_to_dict": map_avaliacao_online_to_dict,
    "map_avaliacao_online_page_to_dict": map_avaliacao_online_page_to_dict,
    "map_avaliacao_online_question_to_dict": map_avaliacao_online_question_to_dict,
    "map_avaliacao_online_question_alternative_to_dict": map_avaliacao_online_question_alternative_to_dict,
    "map_avaliacao_teste_to_dict": map_avaliacao_teste_to_dict,
    "map_dados_to_dict": map_dados_to_dict,
    "map_disciplina_to_dict": map_disciplina_to_dict,
    "map_envios_tutor_mensagens_to_dict": map_envios_tutor_mensagens_to_dict,
    "map_escola_to_dict": map_escola_to_dict,
    "map_estados_to_dict": map_estados_to_dict,
    "map_external_reports_to_dict": map_external_reports_to_dict,
    "map_forget_password_to_dict": map_forget_password_to_dict,
    "map_formacao_to_dict": map_formacao_to_dict,
    "map_genero_to_dict": map_genero_to_dict,
    "map_importar_dados_to_dict": map_importar_dados_to_dict,
    "map_infrequencia_to_dict": map_infrequencia_to_dict,
    "map_job_to_dict": map_job_to_dict,
    "map_matriz_referencia_to_dict": map_matriz_referencia_to_dict,
    "map_matriz_referencia_serie_to_dict": map_matriz_referencia_serie_to_dict,
    "map_matriz_referencia_topico_to_dict": map_matriz_referencia_topico_to_dict,
    "map_matriz_referencia_topico_items_to_dict": map_matriz_referencia_topico_items_to_dict,
    "map_messages_to_dict": map_messages_to_dict,
    "map_messages_municipios_municipio_to_dict": map_messages_municipios_municipio_to_dict,
    "map_messages_schools_escola_to_dict": map_messages_schools_escola_to_dict,
    "map_microdata_to_dict": map_microdata_to_dict,
    "map_migrations_to_dict": map_migrations_to_dict,
    "map_municipio_to_dict": map_municipio_to_dict,
    "map_notifications_to_dict": map_notifications_to_dict,
    "map_pcd_to_dict": map_pcd_to_dict,
    "map_perfil_base_to_dict": map_perfil_base_to_dict,
    "map_professor_to_dict": map_professor_to_dict,
    "map_raca_to_dict": map_raca_to_dict,
    "map_regionais_to_dict": map_regionais_to_dict,
    "map_report_descriptor_to_dict": map_report_descriptor_to_dict,
    "map_report_edition_to_dict": map_report_edition_to_dict,
    "map_report_not_evaluated_to_dict": map_report_not_evaluated_to_dict,
    "map_report_question_to_dict": map_report_question_to_dict,
    "map_report_question_option_to_dict": map_report_question_option_to_dict,
    "map_report_race_to_dict": map_report_race_to_dict,
    "map_report_subject_to_dict": map_report_subject_to_dict,
    "map_series_to_dict": map_series_to_dict,
    "map_sub_perfil_to_dict": map_sub_perfil_to_dict,
    "map_sub_perfil_area_to_dict": map_sub_perfil_area_to_dict,
    "map_system_logs_to_dict": map_system_logs_to_dict,
    "map_templates_mensagens_to_dict": map_templates_mensagens_to_dict,
    "map_teste_to_dict": map_teste_to_dict,
    "map_teste_gabarito_to_dict": map_teste_gabarito_to_dict,
    "map_transferencia_to_dict": map_transferencia_to_dict,
    "map_turma_to_dict": map_turma_to_dict,
    "map_turma_aluno_to_dict": map_turma_aluno_to_dict,
    "map_turma_professor_to_dict": map_turma_professor_to_dict,
    "map_tutor_mensagens_to_dict": map_tutor_mensagens_to_dict,
    "map_usuario_to_dict": map_usuario_to_dict,
    }