--  Copyright 2025 TecOnca Data Solutions.


select 
	* 
from (
select 'aluno' as tabela_nome, 'ALU_DT_CRIACAO' as coluna_nome, MAX(ALU_DT_CRIACAO) from Saev.aluno
  Union All
select 'aluno' as tabela_nome, 'ALU_DT_ATUALIZACAO' as coluna_nome, MAX(ALU_DT_ATUALIZACAO) from Saev.aluno
  Union All
select 'aluno_teste' as tabela_nome, 'ALT_DT_CRIACAO' as coluna_nome, MAX(ALT_DT_CRIACAO) from Saev.aluno_teste
  Union All
select 'aluno_teste' as tabela_nome, 'ALT_DT_ATUALIZACAO' as coluna_nome, MAX(ALT_DT_ATUALIZACAO) from Saev.aluno_teste
  Union All
select 'aluno_teste_resposta' as tabela_nome, 'ATR_DT_CRIACAO' as coluna_nome, MAX(ATR_DT_CRIACAO) from Saev.aluno_teste_resposta
  Union All
select 'aluno_teste_resposta_historico' as tabela_nome, 'ATH_DT_CRIACAO' as coluna_nome, MAX(ATH_DT_CRIACAO) from Saev.aluno_teste_resposta_historico
  Union All
select 'ano_letivo' as tabela_nome, 'ANO_DT_CRIACAO' as coluna_nome, MAX(ANO_DT_CRIACAO) from Saev.ano_letivo
  Union All
select 'ano_letivo' as tabela_nome, 'ANO_DT_ATUALIZACAO' as coluna_nome, MAX(ANO_DT_ATUALIZACAO) from Saev.ano_letivo
  Union All
select 'area' as tabela_nome, 'ARE_DT_CRIACAO' as coluna_nome, MAX(ARE_DT_CRIACAO) from Saev.area
  Union All
select 'area' as tabela_nome, 'ARE_DT_ATUALIZACAO' as coluna_nome, MAX(ARE_DT_ATUALIZACAO) from Saev.area
  Union All
select 'avaliacao' as tabela_nome, 'AVA_DT_CRIACAO' as coluna_nome, MAX(AVA_DT_CRIACAO) from Saev.avaliacao
  Union All
select 'avaliacao' as tabela_nome, 'AVA_DT_ATUALIZACAO' as coluna_nome, MAX(AVA_DT_ATUALIZACAO) from Saev.avaliacao
  Union All
select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_CRIACAO' as coluna_nome, MAX(AVM_DT_CRIACAO) from Saev.avaliacao_municipio
  Union All
select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_ATUALIZACAO' as coluna_nome, MAX(AVM_DT_ATUALIZACAO) from Saev.avaliacao_municipio
  Union All
select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_INICIO' as coluna_nome, MAX(AVM_DT_INICIO) from Saev.avaliacao_municipio
  Union All
select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_FIM' as coluna_nome, MAX(AVM_DT_FIM) from Saev.avaliacao_municipio
  Union All
select 'avaliacao_municipio' as tabela_nome, 'AVM_DT_DISPONIVEL' as coluna_nome, MAX(AVM_DT_DISPONIVEL) from Saev.avaliacao_municipio
  Union All
select 'avaliacao_online' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.avaliacao_online
  Union All
select 'avaliacao_online' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.avaliacao_online
  Union All
select 'avaliacao_online_page' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.avaliacao_online_page
  Union All
select 'avaliacao_online_page' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.avaliacao_online_page
  Union All
select 'avaliacao_online_question' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.avaliacao_online_question
  Union All
select 'avaliacao_online_question' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.avaliacao_online_question
  Union All
select 'avaliacao_online_question_alternative' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.avaliacao_online_question_alternative
  Union All
select 'avaliacao_online_question_alternative' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.avaliacao_online_question_alternative
  Union All
select 'disciplina' as tabela_nome, 'DIS_DT_CRIACAO' as coluna_nome, MAX(DIS_DT_CRIACAO) from Saev.disciplina
  Union All
select 'disciplina' as tabela_nome, 'DIS_DT_ATUALIZACAO' as coluna_nome, MAX(DIS_DT_ATUALIZACAO) from Saev.disciplina
  Union All
select 'escola' as tabela_nome, 'ESC_DT_CRIACAO' as coluna_nome, MAX(ESC_DT_CRIACAO) from Saev.escola
  Union All
select 'escola' as tabela_nome, 'ESC_DT_ATUALIZACAO' as coluna_nome, MAX(ESC_DT_ATUALIZACAO) from Saev.escola
  Union All
select 'estados' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.estados
  Union All
select 'estados' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.estados
  Union All
select 'forget_password' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.forget_password
  Union All
select 'forget_password' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.forget_password
  Union All
select 'formacao' as tabela_nome, 'FOR_DT_CRIACAO' as coluna_nome, MAX(FOR_DT_CRIACAO) from Saev.formacao
  Union All
select 'formacao' as tabela_nome, 'FOR_DT_ATUALIZACAO' as coluna_nome, MAX(FOR_DT_ATUALIZACAO) from Saev.formacao
  Union All
select 'genero' as tabela_nome, 'GEN_DT_CRIACAO' as coluna_nome, MAX(GEN_DT_CRIACAO) from Saev.genero
  Union All
select 'genero' as tabela_nome, 'GEN_DT_ATUALIZACAO' as coluna_nome, MAX(GEN_DT_ATUALIZACAO) from Saev.genero
  Union All
select 'infrequencia' as tabela_nome, 'IFR_DT_CRIACAO' as coluna_nome, MAX(IFR_DT_CRIACAO) from Saev.infrequencia
  Union All
select 'infrequencia' as tabela_nome, 'IFR_DT_ATUALIZACAO' as coluna_nome, MAX(IFR_DT_ATUALIZACAO) from Saev.infrequencia
  Union All
select 'job' as tabela_nome, 'startDate' as coluna_nome, MAX(startDate) from Saev.job
  Union All
select 'job' as tabela_nome, 'endDate' as coluna_nome, MAX(endDate) from Saev.job
  Union All
select 'job' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.job
  Union All
select 'job' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.job
  Union All
select 'matriz_referencia' as tabela_nome, 'MAR_DT_CRIACAO' as coluna_nome, MAX(MAR_DT_CRIACAO) from Saev.matriz_referencia
  Union All
select 'matriz_referencia' as tabela_nome, 'MAR_DT_ATUALIZACAO' as coluna_nome, MAX(MAR_DT_ATUALIZACAO) from Saev.matriz_referencia
  Union All
select 'matriz_referencia_topico' as tabela_nome, 'MTO_DT_CRIACAO' as coluna_nome, MAX(MTO_DT_CRIACAO) from Saev.matriz_referencia_topico
  Union All
select 'matriz_referencia_topico' as tabela_nome, 'MTO_DT_ATUALIZACAO' as coluna_nome, MAX(MTO_DT_ATUALIZACAO) from Saev.matriz_referencia_topico
  Union All
select 'matriz_referencia_topico_items' as tabela_nome, 'MTI_DT_CRIACAO' as coluna_nome, MAX(MTI_DT_CRIACAO) from Saev.matriz_referencia_topico_items
  Union All
select 'matriz_referencia_topico_items' as tabela_nome, 'MTI_DT_ATUALIZACAO' as coluna_nome, MAX(MTI_DT_ATUALIZACAO) from Saev.matriz_referencia_topico_items
  Union All
select 'messages' as tabela_nome, 'MEN_DT_CRIACAO' as coluna_nome, MAX(MEN_DT_CRIACAO) from Saev.messages
  Union All
select 'messages' as tabela_nome, 'MEN_DT_ATUALIZACAO' as coluna_nome, MAX(MEN_DT_ATUALIZACAO) from Saev.messages
  Union All
select 'municipio' as tabela_nome, 'MUN_DT_INICIO' as coluna_nome, MAX(MUN_DT_INICIO) from Saev.municipio
  Union All
select 'municipio' as tabela_nome, 'MUN_DT_FIM' as coluna_nome, MAX(MUN_DT_FIM) from Saev.municipio
  Union All
select 'municipio' as tabela_nome, 'MUN_DT_CRIACAO' as coluna_nome, MAX(MUN_DT_CRIACAO) from Saev.municipio
  Union All
select 'municipio' as tabela_nome, 'MUN_DT_ATUALIZACAO' as coluna_nome, MAX(MUN_DT_ATUALIZACAO) from Saev.municipio
  Union All
select 'pcd' as tabela_nome, 'PCD_DT_CRIACAO' as coluna_nome, MAX(PCD_DT_CRIACAO) from Saev.pcd
  Union All
select 'pcd' as tabela_nome, 'PCD_DT_ATUALIZACAO' as coluna_nome, MAX(PCD_DT_ATUALIZACAO) from Saev.pcd
  Union All
select 'perfil_base' as tabela_nome, 'PER_DT_CRIACAO' as coluna_nome, MAX(PER_DT_CRIACAO) from Saev.perfil_base
  Union All
select 'perfil_base' as tabela_nome, 'PER_DT_ATUALIZACAO' as coluna_nome, MAX(PER_DT_ATUALIZACAO) from Saev.perfil_base
  Union All
select 'professor' as tabela_nome, 'PRO_DT_NASC' as coluna_nome, MAX(PRO_DT_NASC) from Saev.professor
  Union All
select 'professor' as tabela_nome, 'PRO_DT_CRIACAO' as coluna_nome, MAX(PRO_DT_CRIACAO) from Saev.professor
  Union All
select 'professor' as tabela_nome, 'PRO_DT_ATUALIZACAO' as coluna_nome, MAX(PRO_DT_ATUALIZACAO) from Saev.professor
  Union All
select 'raca' as tabela_nome, 'PEL_DT_CRIACAO' as coluna_nome, MAX(PEL_DT_CRIACAO) from Saev.raca
  Union All
select 'raca' as tabela_nome, 'PEL_DT_ATUALIZACAO' as coluna_nome, MAX(PEL_DT_ATUALIZACAO) from Saev.raca
  Union All
select 'regionais' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.regionais
  Union All
select 'regionais' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.regionais
  Union All
select 'report_descriptor' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_descriptor
  Union All
select 'report_descriptor' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_descriptor
  Union All
select 'report_edition' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_edition
  Union All
select 'report_edition' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_edition
  Union All
select 'report_not_evaluated' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_not_evaluated
  Union All
select 'report_not_evaluated' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_not_evaluated
  Union All
select 'report_race' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_race
  Union All
select 'report_race' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_race
  Union All
select 'report_subject' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.report_subject
  Union All
select 'report_subject' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.report_subject
  Union All
select 'series' as tabela_nome, 'SER_DT_CRIACAO' as coluna_nome, MAX(SER_DT_CRIACAO) from Saev.series
  Union All
select 'series' as tabela_nome, 'SER_DT_ATUALIZACAO' as coluna_nome, MAX(SER_DT_ATUALIZACAO) from Saev.series
  Union All
select 'sub_perfil' as tabela_nome, 'SPE_DT_CRIACAO' as coluna_nome, MAX(SPE_DT_CRIACAO) from Saev.sub_perfil
  Union All
select 'sub_perfil' as tabela_nome, 'SPE_DT_ATUALIZACAO' as coluna_nome, MAX(SPE_DT_ATUALIZACAO) from Saev.sub_perfil
  Union All
select 'teste' as tabela_nome, 'TES_DT_CRIACAO' as coluna_nome, MAX(TES_DT_CRIACAO) from Saev.teste
  Union All
select 'teste' as tabela_nome, 'TES_DT_ATUALIZACAO' as coluna_nome, MAX(TES_DT_ATUALIZACAO) from Saev.teste
  Union All
select 'teste_gabarito' as tabela_nome, 'TEG_DT_CRIACAO' as coluna_nome, MAX(TEG_DT_CRIACAO) from Saev.teste_gabarito
  Union All
select 'teste_gabarito' as tabela_nome, 'TEG_DT_ATUALIZACAO' as coluna_nome, MAX(TEG_DT_ATUALIZACAO) from Saev.teste_gabarito
  Union All
select 'transferencia' as tabela_nome, 'TRF_DT_CRIACAO' as coluna_nome, MAX(TRF_DT_CRIACAO) from Saev.transferencia
  Union All
select 'transferencia' as tabela_nome, 'TRF_DT_ATUALIZACAO' as coluna_nome, MAX(TRF_DT_ATUALIZACAO) from Saev.transferencia
  Union All
select 'turma' as tabela_nome, 'TUR_DT_CRIACAO' as coluna_nome, MAX(TUR_DT_CRIACAO) from Saev.turma
  Union All
select 'turma' as tabela_nome, 'TUR_DT_ATUALIZACAO' as coluna_nome, MAX(TUR_DT_ATUALIZACAO) from Saev.turma
  Union All
select 'turma_aluno' as tabela_nome, 'startDate' as coluna_nome, MAX(startDate) from Saev.turma_aluno
  Union All
select 'turma_aluno' as tabela_nome, 'endDate' as coluna_nome, MAX(endDate) from Saev.turma_aluno
  Union All
select 'turma_aluno' as tabela_nome, 'createdAt' as coluna_nome, MAX(createdAt) from Saev.turma_aluno
  Union All
select 'turma_aluno' as tabela_nome, 'updatedAt' as coluna_nome, MAX(updatedAt) from Saev.turma_aluno
  Union All
select 'usuario' as tabela_nome, 'USU_DT_CRIACAO' as coluna_nome, MAX(USU_DT_CRIACAO) from Saev.usuario
  Union All
select 'usuario' as tabela_nome, 'USU_DT_ATUALIZACAO' as coluna_nome, MAX(USU_DT_ATUALIZACAO) from Saev.usuario
)