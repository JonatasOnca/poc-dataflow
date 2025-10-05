from datetime import datetime

# Sua variável string de entrada
variavel_string = 'Timestamp(1675212635.323000)'

# 1. Extrai a parte numérica da string usando fatiamento (slicing)
#    Começa no caractere 10 (após 'Timestamp(') e vai até o último caractere (antes de ')')
numero_em_string = variavel_string[10:-1]

# 2. Converte a string extraída para um número float.
#    É CRUCIAL usar float() para manter os microssegundos.
timestamp_numerico = float(numero_em_string)

# 3. Cria o objeto datetime a partir do timestamp numérico
objeto_datetime = datetime.fromtimestamp(timestamp_numerico)


print(f"String Original    : {variavel_string}")
print(f"Número Extraído    : {timestamp_numerico}")
print(f"Objeto Datetime    : {objeto_datetime}")

# Formatando para uma visualização mais clara
print(f"Data Formatada    : {objeto_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}")