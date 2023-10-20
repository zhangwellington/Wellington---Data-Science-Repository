def tratamento():
	# **Tratamento PORTO BATIMENTO (sem ser Segunda)**
	
	import pandas as pd
	import numpy as np
	import datetime
	import os
	import time
	import sys
	
	today=datetime.date.today()
	#Checando se não é segunda
	if today.weekday()==0:
	  print('Hoje é segunda! Rodar outro script!')
	  sys.exit()
	
	##Mover arquivos ao finalizar o tratamento?
	
	mover=True
	
	from google.colab import drive
	drive.mount('/content/drive')
	
	carteira='Porto Batimento'
	
	#Definindo diretórios
	diretorio_tratamento=r'/content/drive/MyDrive/Processamentos/Tratamento/'
	diretorio=diretorio_tratamento + carteira + '/'
	output='/'.join(['/content/drive/MyDrive/Pronto pra upload',str(datetime.date.today().year),str(datetime.date.today().month).zfill(2),datetime.date.today().strftime('%d-%m-%Y')])+'/'
	os.makedirs(output,exist_ok=True)
	processedfiles=diretorio+'processedfiles/'
	ingestion=diretorio+'ingestion/'
	auxiliary=diretorio+'auxiliary/'
	#Nome do arquivo de saída com caminho
	csv_saida=output+'early_porto-batimento_'+str(today.strftime('%d-%m-%Y'))+'.csv'
	
	#Esperando ler 2 arquivos na ingestion
	while len(os.listdir(ingestion))!=2:
	  time.sleep(5)
	print(os.listdir(ingestion))
	
	#Lendo os arquivos da ingestion (TB_DIST e ESC)
	for arquivo in os.listdir(ingestion):
	  if arquivo.find('TB')>=0:
	    nome_tb=arquivo
	    df_tb=pd.read_excel(ingestion+arquivo,dtype=object)
	    print(arquivo,'importado com sucesso!')
	  elif arquivo.find('ESC')>=0:
	    nome_esc=arquivo
	    df_esc=pd.read_csv(ingestion+arquivo,dtype=object)
	    print(arquivo,'importado com sucesso!')
	#Lendo tabela auxiliar concatenada
	df_concatenado=pd.read_csv(auxiliary+os.listdir(auxiliary)[0],dtype=object)
	print(os.listdir(auxiliary)[0],'importado com sucesso!')
	
	#Criando cópia dos arquivos importados para fazer alteração
	tabela=df_tb.copy()
	concatenado=df_concatenado.copy()
	esc=df_esc.copy()
	
	concatenado=pd.concat([concatenado,tabela],join='inner').reset_index().drop('index',axis=1)
	
	col_final=['NR_RAS','NR_COD_ESCRITORIO_NOVO','DT_FECH_SINISTRO','QT_DIAS_ATRASO','VR_DESPESAS_SIN','VR_EM_ATRASO_COM_JUROS','PC_DES_AVISTA','PC_DES_2_3_VEZES','PC_DES_4_6_VEZES',\
	           'PC_DES_7_12_VEZES','PC_DES_13_20_VEZES','PC_DESC_20_VEZES','VR_VALOR_COM_DESPESA','PROPENSAO','NM_GARANTIDO','NR_CPF_V2','NR_CNPJ_V2','NR_CELULAR_PORTO','CD_UF','VR_COBERTURA_ALUGUEL',\
	           'EMAIL_1','DT_NASCIMENTO_INQUILINO']
	
	##Início dos Tratamentos
	
	### Tratamento das Tabelas Auxiliares
	
	#Ordenando por Email e agrupando
	concatenado=concatenado.sort_values('EMAIL_1',ascending=False)
	concatenado=concatenado.groupby('NR_RAS',as_index=False).first()
	
	#Capitalizando nomes
	concatenado['NM_GARANTIDO']=concatenado['NM_GARANTIDO'].str.title()
	
	#Tratando '..' na coluna NR_CELULAR_PORTO de concatenado
	concatenado['NR_CELULAR_PORTO']=concatenado['NR_CELULAR_PORTO'].apply(lambda x: '' if str(x)=='..' else x)
	
	#Deixando 2 dígitos decimais
	concatenado['VR_COBERTURA_ALUGUEL']=concatenado['VR_COBERTURA_ALUGUEL'].astype(float)
	concatenado['VR_COBERTURA_ALUGUEL']=concatenado['VR_COBERTURA_ALUGUEL'].round(2)
	concatenado['VR_COBERTURA_ALUGUEL']=concatenado['VR_COBERTURA_ALUGUEL'].map('{:.2f}'.format)
	
	#Tratando números de telefone
	concatenado['NR_CELULAR_PORTO']=concatenado['NR_CELULAR_PORTO'].apply(lambda x: str(x).split('.')[0] if str(x).find('.')>=0 else x)
	
	### Tratamento de ESC
	
	#Excluindo colunas indesejadas
	excluir=[]
	for coluna in esc.columns.to_list():
	  if coluna not in col_final:
	    excluir.append(coluna)
	esc=esc.drop(excluir, axis=1)
	esc.head()
	
	#Tratando com separador de milhares como '.' e de decimal como ','
	tratar_ponto_virgula=['VR_DESPESAS_SIN','VR_EM_ATRASO_COM_JUROS','VR_VALOR_COM_DESPESA']
	
	for coluna in tratar_ponto_virgula:
	  esc[coluna]=esc[coluna].apply(lambda x: str(x).replace('.',''))
	  esc[coluna]=esc[coluna].apply(lambda x: str(x).replace(',','.'))
	  esc[coluna]=esc[coluna]=esc[coluna]=esc[coluna].astype(float)
	  print(esc[coluna])
	
	### Mesclando as duas tabelas
	
	#Fazendo o merge das Tabelas
	merged=esc.merge(concatenado,on='NR_RAS',how='inner')
	
	#Eliminando colunas indesejadas
	merged=merged[col_final]
	
	###Tratando os números de telefone e email
	
	#Validação de Celular
	col_celular='NR_CELULAR_PORTO'
	filtro=np.char.str_len(np.array(merged[col_celular]).astype(str))!=11
	merged.loc[filtro,col_celular]=''
	
	#Validação de Email
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
	
	#Exportando Tabela Auxiliar para auxiliary
	concatenado.to_csv(auxiliary+'Concatenado.csv',index=False)
	#Exportando o arquivo para Output
	merged.to_csv(csv_saida,index=False)
	
	##Movendo arquivos
	
	#Movendo arquivos para processedfiles
	if mover:
	  arquivos=[nome_esc,nome_tb]
	  for arquivo in arquivos:
	    try:
	      os.rename(ingestion + arquivo, processedfiles + arquivo)
	    except:
	      os.remove(processedfiles + arquivo)
	      os.rename(ingestion + arquivo, processedfiles + arquivo)