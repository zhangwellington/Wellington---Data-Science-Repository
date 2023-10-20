def tratamento():
	# **Tratamento PORTO ESSENCIAL**
	
	import pandas as pd
	import numpy as np
	import datetime
	import os
	import time
	
	##Mover arquivos ao finalizar o tratamento?
	
	mover=True
	
	from google.colab import drive
	drive.mount('/content/drive')
	
	carteira='Porto Essencial'
	
	#Definindo diretórios
	diretorio_tratamento=r'/content/drive/MyDrive/Processamentos/Tratamento/'
	diretorio=diretorio_tratamento + carteira + '/'
	output='/'.join(['/content/drive/MyDrive/Pronto pra upload',str(datetime.date.today().year),str(datetime.date.today().month).zfill(2),datetime.date.today().strftime('%d-%m-%Y')])+'/'
	os.makedirs(output,exist_ok=True)
	processedfiles=diretorio+'processedfiles/'
	ingestion=diretorio+'ingestion/'
	
	#Nome do arquivo de saída com caminho
	csv_saida=output+'early_porto-essencial_'+str(datetime.date.today().strftime('%d-%m-%Y'))+'.csv'
	
	#Esperando ler 2 arquivos na ingestion
	while len(os.listdir(ingestion))!=2:
	  time.sleep(5)
	print(os.listdir(ingestion))
	
	#Lendo os 2 arquivos
	for arquivo in os.listdir(ingestion):
	  if arquivo.find('ESC_ESSEN')>=0:
	    print('ESC_ESSEN:',arquivo)
	    nome_essen=arquivo
	    esc_essen=pd.read_csv(ingestion+arquivo,dtype=object)
	  elif arquivo.find('TABELA_ESSENCIAL')>=0:
	    nome_tab_essen=arquivo
	    tab_essen=pd.read_excel(ingestion+arquivo,dtype=object)
	    print('TABELA_ESSENCIAL:',arquivo)
	
	##Início dos Tratamentos
	
	### Tratamento de TABELA_ESSENCIAL
	
	#Listando colunas repetidas da tabela auxiliar diferente de NR_RAS
	col_repetida=[]
	for coluna in tab_essen.columns.to_list():
	    if coluna in esc_essen.columns.to_list() and coluna!='NR_RAS':
	        col_repetida.append(coluna)
	
	print('Colunas repetidas:') #Resultados anteriores: ['DT_FECH_SINISTRO', 'VR_DESPESAS_SIN']
	print(col_repetida)
	
	#Capitalizando nomes
	tab_essen['NM_GARANTIDO']=tab_essen['NM_GARANTIDO'].str.title()
	
	#Excluindo colunas repetidas de tab_essen
	tab_essen.drop(col_repetida,axis=1,inplace=True)
	
	#Tratando '..' na coluna NR_CELULAR_PORTO de tab_essen
	tab_essen['NR_CELULAR_PORTO']=tab_essen['NR_CELULAR_PORTO'].apply(lambda x: '' if str(x)=='..' else x)
	
	#Excluindo colunas indesejadas
	col_final=['NR_RAS','NR_COD_ESCRITORIO_NOVO','DT_FECH_SINISTRO','QT_DIAS_ATRASO','VR_DESPESAS_SIN','VR_EM_ATRASO_COM_JUROS','PC_DES_AVISTA',\
	                    'PC_DES_2_3_VEZES','PC_DES_4_6_VEZES','PC_DES_7_12_VEZES','PC_DES_13_20_VEZES','PC_DESC_20_VEZES','VR_VALOR_COM_DESPESA','PROPENSAO',\
	                    'NM_GARANTIDO','NR_CPF_V2','NR_CNPJ_V2','NR_CELULAR_PORTO','CD_UF','VR_COBERTURA_ALUGUEL','EMAIL_1','DT_NASCIMENTO_INQUILINO']
	excluir=[]
	for coluna in tab_essen.columns.to_list():
	  if coluna not in col_final:
	    excluir.append(coluna)
	tab_essen=tab_essen.drop(excluir, axis=1)
	
	#Ordenando por email e agrupando por NR_RAS
	tab_essen=tab_essen.sort_values('EMAIL_1',ascending=False)
	tab_essen=tab_essen.groupby('NR_RAS',as_index=False).first()
	
	#Convertendo datas pro formato brasileiro
	for coluna in tab_essen.select_dtypes(include='datetime').columns.tolist():
	    tab_essen[coluna]=tab_essen[coluna].apply(lambda x: str(x)[:10])
	    tab_essen[coluna]=pd.to_datetime(tab_essen[coluna])
	    tab_essen[coluna]=tab_essen[coluna].dt.strftime('%d/%m/%Y')
	
	### Tratamento de ESC_ESSEN
	
	#Tratando com separador de milhares como '.' e de decimal como ','
	tratar_ponto_virgula=['VR_EM_ATRASO_COM_JUROS','VR_VALOR_COM_DESPESA']
	
	for coluna in tratar_ponto_virgula:
	  esc_essen[coluna]=esc_essen[coluna].apply(lambda x: str(x).replace('.',''))
	  esc_essen[coluna]=esc_essen[coluna].apply(lambda x: str(x).replace(',','.'))
	  esc_essen[coluna]=esc_essen[coluna]=esc_essen[coluna]=esc_essen[coluna].astype(float)
	  print(esc_essen[coluna])
	
	### Mesclando as duas tabelas
	
	#Fazendo o merge das Tabelas
	merged=esc_essen.merge(tab_essen,on='NR_RAS',how='inner')
	
	#Eliminando colunas indesejadas
	merged=merged[col_final]
	
	###Tratando os números de telefone e email
	
	#Validação de Celular
	col_celular='NR_CELULAR_PORTO'
	filtro=np.char.str_len(np.array(merged[col_celular]).astype(str))!=11
	merged.loc[filtro,col_celular]=''
	#Aplicando lower e strip na coluna de email
	coluna_email='EMAIL_1'
	merged[coluna_email]=merged[coluna_email].str.lower().str.strip()
	#Apagando emails não validados
	merged[coluna_email].loc[merged[coluna_email].str.find('@')<0]=''
	merged['num @']=merged[coluna_email].str.len()-merged[coluna_email].str.replace('@','').str.len()
	filtro=merged['num @']!=1
	merged.loc[filtro,coluna_email]=''
	merged.drop('num @',axis=1,inplace=True)
	
	###Tratar coluna de Nome
	
	coluna_nomes=['NM_GARANTIDO']
	for coluna_nome in coluna_nomes:
	  merged[coluna_nome]=merged[coluna_nome].str.strip().str.title()
	
	##Exportando
	
	#Exportando o arquivo para Output
	merged.to_csv(csv_saida,index=False)
	
	##Movendo arquivos
	
	#Movendo arquivos para processedfiles
	if mover:
	  arquivos=[nome_tab_essen,nome_essen]
	  for arquivo in arquivos:
	    try:
	      os.rename(ingestion + arquivo, processedfiles + arquivo)
	    except:
	      os.remove(processedfiles + arquivo)
	      os.rename(ingestion + arquivo, processedfiles + arquivo)