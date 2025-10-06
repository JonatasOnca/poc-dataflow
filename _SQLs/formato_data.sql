select 
	* 
from (
	select 'aluno' as tabela_nome, 'ALU_DT_CRIACAO' as coluna_nome, MAX(ALU_DT_CRIACAO) from Saev.aluno
	UNION ALL
	select 'aluno' as tabela_nome, 'ALU_DT_ATUALIZACAO' as coluna_nome, MAX(ALU_DT_ATUALIZACAO) from Saev.aluno
	UNION ALL
	select 'aluno_teste' as tabela_nome, 'ALT_DT_CRIACAO' as coluna_nome, MAX(ALT_DT_CRIACAO) from Saev.aluno_teste
	UNION ALL
	select 'aluno_teste' as tabela_nome, 'ALT_DT_ATUALIZACAO' as coluna_nome, MAX(ALT_DT_ATUALIZACAO) from Saev.aluno_teste
	UNION ALL
	select 'aluno_teste_resposta' as tabela_nome, 'ATR_DT_CRIACAO' as coluna_nome, MAX(ATR_DT_CRIACAO) from Saev.aluno_teste_resposta
	UNION ALL
	select 'aluno_teste_resposta_historico' as tabela_nome, 'ATH_DT_CRIACAO' as coluna_nome, MAX(ATH_DT_CRIACAO) from Saev.aluno_teste_resposta_historico
	UNION ALL
	select 'ano_letivo' as tabela_nome, 'ANO_DT_CRIACAO' as coluna_nome, MAX(ANO_DT_CRIACAO) from Saev.ano_letivo
	UNION ALL
	select 'ano_letivo' as tabela_nome, 'ANO_DT_ATUALIZACAO' as coluna_nome, MAX(ANO_DT_ATUALIZACAO) from Saev.ano_letivo
	UNION ALL
	select 'area' as tabela_nome, 'ARE_DT_CRIACAO' as coluna_nome, MAX(ARE_DT_CRIACAO) from Saev.area
	UNION ALL
	select 'area' as tabela_nome, 'ARE_DT_ATUALIZACAO' as coluna_nome, MAX(ARE_DT_ATUALIZACAO) from Saev.area
	UNION ALL
	select 'arquivo' as tabela_nome, 'ARQ_DT_CRIACAO' as coluna_nome, MAX(ARQ_DT_CRIACAO) from Saev.arquivo
	UNION ALL
	select 'arquivo' as tabela_nome, 'ARQ_DT_ATUALIZACAO' as coluna_nome, MAX(ARQ_DT_ATUALIZACAO) from Saev.arquivo
	UNION ALL
	select 'avaliacao' as tabela_nome, 'AVA_DT_CRIACAO' as coluna_nome, MAX(AVA_DT_CRIACAO) from Saev.avaliacao
	UNION ALL
	select 'avaliacao' as tabela_nome, 'AVA_DT_ATUALIZACAO' as coluna_nome, MAX(AVA_DT_ATUALIZACAO) from Saev.avaliacao
	UNION ALL
	select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_CRIACAO' as coluna_nome, MAX(AVM_DT_CRIACAO) from Saev.avaliacao_municipio
	UNION ALL
	select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_ATUALIZACAO' as coluna_nome, MAX(AVM_DT_ATUALIZACAO) from Saev.avaliacao_municipio
	UNION ALL
	select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_INICIO' as coluna_nome, MAX(AVM_DT_INICIO) from Saev.avaliacao_municipio
	UNION ALL
	select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_FIM' as coluna_nome, MAX(AVM_DT_FIM) from Saev.avaliacao_municipio
	UNION ALL
	select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_DISPONIVEL' as coluna_nome, MAX(AVM_DT_DISPONIVEL) from Saev.avaliacao_municipio
	UNION ALL
	select 'avaliacao_online' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.avaliacao_online
	UNION ALL
	select 'avaliacao_online' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.avaliacao_online
	UNION ALL
	select 'avaliacao_online_page' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.avaliacao_online_page
	UNION ALL
	select 'avaliacao_online_page' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.avaliacao_online_page
	UNION ALL
	select 'avaliacao_online_question' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.avaliacao_online_question
	UNION ALL
	select 'avaliacao_online_question' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.avaliacao_online_question
	UNION ALL
	select 'avaliacao_online_question_alternative' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.avaliacao_online_question_alternative
	UNION ALL
	select 'avaliacao_online_question_alternative' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.avaliacao_online_question_alternative
	UNION ALL
	select 'dados' as tabela_nome, 'DAT_DT_CRIACAO' as coluna_nome, MAX(DAT_DT_CRIACAO) from Saev.dados
	UNION ALL
	select 'dados' as tabela_nome, 'DAT_DT_ATUALIZACAO' as coluna_nome, MAX(DAT_DT_ATUALIZACAO) from Saev.dados
	UNION ALL
	select 'disciplina' as tabela_nome, 'DIS_DT_CRIACAO' as coluna_nome, MAX(DIS_DT_CRIACAO) from Saev.disciplina
	UNION ALL
	select 'disciplina' as tabela_nome, 'DIS_DT_ATUALIZACAO' as coluna_nome, MAX(DIS_DT_ATUALIZACAO) from Saev.disciplina
	UNION ALL
	select 'envios_tutor_mensagens' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.envios_tutor_mensagens
	UNION ALL
	select 'escola' as tabela_nome, 'ESC_DT_CRIACAO' as coluna_nome, MAX(ESC_DT_CRIACAO) from Saev.escola
	UNION ALL
	select 'escola' as tabela_nome, 'ESC_DT_ATUALIZACAO' as coluna_nome, MAX(ESC_DT_ATUALIZACAO) from Saev.escola
	UNION ALL
	select 'estados' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.estados
	UNION ALL
	select 'estados' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.estados
	UNION ALL
	select 'external_reports' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.external_reports
	UNION ALL
	select 'external_reports' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.external_reports
	UNION ALL
	select 'forget_password' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.forget_password
	UNION ALL
	select 'forget_password' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.forget_password
	UNION ALL
	select 'formacao' as tabela_nome, 'FOR_DT_CRIACAO' as coluna_nome, MAX(FOR_DT_CRIACAO) from Saev.formacao
	UNION ALL
	select 'formacao' as tabela_nome, 'FOR_DT_ATUALIZACAO' as coluna_nome, MAX(FOR_DT_ATUALIZACAO) from Saev.formacao
	UNION ALL
	select 'genero' as tabela_nome, 'GEN_DT_CRIACAO' as coluna_nome, MAX(GEN_DT_CRIACAO) from Saev.genero
	UNION ALL
	select 'genero' as tabela_nome, 'GEN_DT_ATUALIZACAO' as coluna_nome, MAX(GEN_DT_ATUALIZACAO) from Saev.genero
	UNION ALL
	select 'importar_dados' as tabela_nome, 'DAT_DT_CRIACAO' as coluna_nome, MAX(DAT_DT_CRIACAO) from Saev.importar_dados
	UNION ALL
	select 'importar_dados' as tabela_nome, 'DAT_DT_ATUALIZACAO' as coluna_nome, MAX(DAT_DT_ATUALIZACAO) from Saev.importar_dados
	UNION ALL
	select 'infrequencia' as tabela_nome, 'IFR_DT_CRIACAO' as coluna_nome, MAX(IFR_DT_CRIACAO) from Saev.infrequencia
	UNION ALL
	select 'infrequencia' as tabela_nome, 'IFR_DT_ATUALIZACAO' as coluna_nome, MAX(IFR_DT_ATUALIZACAO) from Saev.infrequencia
	UNION ALL
	select 'job' as tabela_nome, 'startDate' as coluna_nome, MAX(startDate) from Saev.job
	UNION ALL
	select 'job' as tabela_nome, 'endDate' as coluna_nome, MAX(endDate) from Saev.job
	UNION ALL
	select 'job' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.job
	UNION ALL
	select 'job' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.job
	UNION ALL
	select 'matriz_referencia' as tabela_nome, 'MAR_DT_CRIACAO' as coluna_nome, MAX(MAR_DT_CRIACAO) from Saev.matriz_referencia
	UNION ALL
	select 'matriz_referencia' as tabela_nome, 'MAR_DT_ATUALIZACAO' as coluna_nome, MAX(MAR_DT_ATUALIZACAO) from Saev.matriz_referencia
	UNION ALL
	select 'matriz_referencia_topico' as tabela_nome, 'MTO_DT_CRIACAO' as coluna_nome, MAX(MTO_DT_CRIACAO) from Saev.matriz_referencia_topico
	UNION ALL
	select 'matriz_referencia_topico' as tabela_nome, 'MTO_DT_ATUALIZACAO' as coluna_nome, MAX(MTO_DT_ATUALIZACAO) from Saev.matriz_referencia_topico
	UNION ALL
	select 'matriz_referencia_topico_items' as tabela_nome, 'MTI_DT_CRIACAO' as coluna_nome, MAX(MTI_DT_CRIACAO) from Saev.matriz_referencia_topico_items
	UNION ALL
	select 'matriz_referencia_topico_items' as tabela_nome, 'MTI_DT_ATUALIZACAO' as coluna_nome, MAX(MTI_DT_ATUALIZACAO) from Saev.matriz_referencia_topico_items
	UNION ALL
	select 'messages' as tabela_nome, 'MEN_DT_CRIACAO' as coluna_nome, MAX(MEN_DT_CRIACAO) from Saev.messages
	UNION ALL
	select 'messages' as tabela_nome, 'MEN_DT_ATUALIZACAO' as coluna_nome, MAX(MEN_DT_ATUALIZACAO) from Saev.messages
	UNION ALL
	select 'microdata' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.microdata
	UNION ALL
	select 'microdata' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.microdata
	UNION ALL
	select 'municipio' as tabela_nome, 'MUN_DT_INICIO' as coluna_nome, MAX(MUN_DT_INICIO) from Saev.municipio
	UNION ALL
	select 'municipio' as tabela_nome, 'MUN_DT_FIM' as coluna_nome, MAX(MUN_DT_FIM) from Saev.municipio
	UNION ALL
	select 'municipio' as tabela_nome, 'MUN_DT_CRIACAO' as coluna_nome, MAX(MUN_DT_CRIACAO) from Saev.municipio
	UNION ALL
	select 'municipio' as tabela_nome, 'MUN_DT_ATUALIZACAO' as coluna_nome, MAX(MUN_DT_ATUALIZACAO) from Saev.municipio
	UNION ALL
	select 'notifications' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.notifications
	UNION ALL
	select 'notifications' as tabela_nome, 'updateAt' as coluna_nome, MAX(updateAt) from Saev.notifications
	UNION ALL
	select 'pcd' as tabela_nome, 'PCD_DT_CRIACAO' as coluna_nome, MAX(PCD_DT_CRIACAO) from Saev.pcd
	UNION ALL
	select 'pcd' as tabela_nome, 'PCD_DT_ATUALIZACAO' as coluna_nome, MAX(PCD_DT_ATUALIZACAO) from Saev.pcd
	UNION ALL
	select 'perfil_base' as tabela_nome, 'PER_DT_CRIACAO' as coluna_nome, MAX(PER_DT_CRIACAO) from Saev.perfil_base
	UNION ALL
	select 'perfil_base' as tabela_nome, 'PER_DT_ATUALIZACAO' as coluna_nome, MAX(PER_DT_ATUALIZACAO) from Saev.perfil_base
	UNION ALL
	select 'professor' as tabela_nome, 'PRO_DT_NASC' as coluna_nome, MAX(PRO_DT_NASC) from Saev.professor
	UNION ALL
	select 'professor' as tabela_nome, 'PRO_DT_CRIACAO' as coluna_nome, MAX(PRO_DT_CRIACAO) from Saev.professor
	UNION ALL
	select 'professor' as tabela_nome, 'PRO_DT_ATUALIZACAO' as coluna_nome, MAX(PRO_DT_ATUALIZACAO) from Saev.professor
	UNION ALL
	select 'raca' as tabela_nome, 'PEL_DT_CRIACAO' as coluna_nome, MAX(PEL_DT_CRIACAO) from Saev.raca
	UNION ALL
	select 'raca' as tabela_nome, 'PEL_DT_ATUALIZACAO' as coluna_nome, MAX(PEL_DT_ATUALIZACAO) from Saev.raca
	UNION ALL
	select 'regionais' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.regionais
	UNION ALL
	select 'regionais' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.regionais
	UNION ALL
	select 'report_descriptor' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_descriptor
	UNION ALL
	select 'report_descriptor' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_descriptor
	UNION ALL
	select 'report_edition' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_edition
	UNION ALL
	select 'report_edition' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_edition
	UNION ALL
	select 'report_not_evaluated' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_not_evaluated
	UNION ALL
	select 'report_not_evaluated' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_not_evaluated
	UNION ALL
	select 'report_question_option' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_question_option
	UNION ALL
	select 'report_race' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_race
	UNION ALL
	select 'report_race' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_race
	UNION ALL
	select 'report_subject' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_subject
	UNION ALL
	select 'report_subject' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_subject
	UNION ALL
	select 'series' as tabela_nome, 'SER_DT_CRIACAO' as coluna_nome, MAX(SER_DT_CRIACAO) from Saev.series
	UNION ALL
	select 'series' as tabela_nome, 'SER_DT_ATUALIZACAO' as coluna_nome, MAX(SER_DT_ATUALIZACAO) from Saev.series
	UNION ALL
	select 'sub_perfil' as tabela_nome, 'SPE_DT_CRIACAO' as coluna_nome, MAX(SPE_DT_CRIACAO) from Saev.sub_perfil
	UNION ALL
	select 'sub_perfil' as tabela_nome, 'SPE_DT_ATUALIZACAO' as coluna_nome, MAX(SPE_DT_ATUALIZACAO) from Saev.sub_perfil
	UNION ALL
	select 'system_logs' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.system_logs
	UNION ALL
	select 'system_logs' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.system_logs
	UNION ALL
	select 'templates_mensagens' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.templates_mensagens
	UNION ALL
	select 'templates_mensagens' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.templates_mensagens
	UNION ALL
	select 'teste' as tabela_nome, 'TES_DT_CRIACAO' as coluna_nome, MAX(TES_DT_CRIACAO) from Saev.teste
	UNION ALL
	select 'teste' as tabela_nome, 'TES_DT_ATUALIZACAO' as coluna_nome, MAX(TES_DT_ATUALIZACAO) from Saev.teste
	UNION ALL
	select 'teste_gabarito' as tabela_nome, 'TEG_DT_CRIACAO' as coluna_nome, MAX(TEG_DT_CRIACAO) from Saev.teste_gabarito
	UNION ALL
	select 'teste_gabarito' as tabela_nome, 'TEG_DT_ATUALIZACAO' as coluna_nome, MAX(TEG_DT_ATUALIZACAO) from Saev.teste_gabarito
	UNION ALL
	select 'transferencia' as tabela_nome, 'TRF_DT_CRIACAO' as coluna_nome, MAX(TRF_DT_CRIACAO) from Saev.transferencia
	UNION ALL
	select 'transferencia' as tabela_nome, 'TRF_DT_ATUALIZACAO' as coluna_nome, MAX(TRF_DT_ATUALIZACAO) from Saev.transferencia
	UNION ALL
	select 'turma' as tabela_nome, 'TUR_DT_CRIACAO' as coluna_nome, MAX(TUR_DT_CRIACAO) from Saev.turma
	UNION ALL
	select 'turma' as tabela_nome, 'TUR_DT_ATUALIZACAO' as coluna_nome, MAX(TUR_DT_ATUALIZACAO) from Saev.turma
	UNION ALL
	select 'turma_aluno' as tabela_nome, 'startDate' as coluna_nome, MAX(startDate) from Saev.turma_aluno
	UNION ALL
	select 'turma_aluno' as tabela_nome, 'endDate' as coluna_nome, MAX(endDate) from Saev.turma_aluno
	UNION ALL
	select 'turma_aluno' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.turma_aluno
	UNION ALL
	select 'turma_aluno' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.turma_aluno
	UNION ALL
	select 'tutor_mensagens' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.tutor_mensagens
	UNION ALL
	select 'tutor_mensagens' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.tutor_mensagens
	UNION ALL
	select 'usuario' as tabela_nome, 'USU_DT_CRIACAO' as coluna_nome, MAX(USU_DT_CRIACAO) from Saev.usuario
	UNION ALL
	select 'usuario' as tabela_nome, 'USU_DT_ATUALIZACAO' as coluna_nome, MAX(USU_DT_ATUALIZACAO) from Saev.usuario
)