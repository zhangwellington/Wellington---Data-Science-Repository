def tratamento():
	#**Tratamento Porto Batimento (Segunda)**
	
	import pandas as pd
	import numpy as np
	import datetime
	import os
	import time
	import datetime
	import sys
	
	today=datetime.date.today()
	#Checando se é segunda
	if today.weekday()!=0:
	  print('Hoje NÃO é segunda! Rodar outro script!')
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
	csv_saida=output+'early_porto-batimento_'+str(datetime.date.today().strftime('%d-%m-%Y'))+'.csv'
	
	#Esperando ler n arquivos na ingestion
	n=7
	
	while len(os.listdir(ingestion))!=n:
	  time.sleep(5)
	print(os.listdir(ingestion))
	
	#Lendo os 7 arquivos (6 tabelas e 1 csv)
	i=1
	for arquivo in os.listdir(ingestion):
	  if arquivo.find('TB')>=0:
	    if i==1:
	      df=pd.read_excel(ingestion+arquivo,dtype=object)
	      print(arquivo,'importado com sucesso!')
	      i+=1
	    else:
	      df2=pd.read_excel(ingestion+arquivo,dtype=object)
	      df=pd.concat([df,df2],join='outer',ignore_index=False)
	      print(arquivo,'concatenado com sucesso!')
	  elif arquivo.find('ESC')>=0:
	    df_esc=pd.read_csv(ingestion+arquivo,dtype=object)
	    print(arquivo,'importado com sucesso!')
	
	#Criando cópia dos arquivos importados para fazer alteração
	concatenado=df.copy()
	esc=df_esc.copy()
	
	col_final=['NR_RAS','NR_COD_ESCRITORIO_NOVO','DT_FECH_SINISTRO','QT_DIAS_ATRASO','VR_DESPESAS_SIN','VR_EM_ATRASO_COM_JUROS','PC_DES_AVISTA','PC_DES_2_3_VEZES','PC_DES_4_6_VEZES',\
	           'PC_DES_7_12_VEZES','PC_DES_13_20_VEZES','PC_DESC_20_VEZES','VR_VALOR_COM_DESPESA','PROPENSAO','NM_GARANTIDO','NR_CPF_V2','NR_CNPJ_V2','NR_CELULAR_PORTO','CD_UF','VR_COBERTURA_ALUGUEL',\
	           'EMAIL_1','DT_NASCIMENTO_INQUILINO']
	
	##Início dos Tratamentos
	
	### Tratamento das Tabelas Auxiliares
	
	#Ordenando por Email e Mantendo o primeiro
	concatenado=concatenado.sort_values('EMAIL_1',ascending=False)
	concatenado=concatenado.drop_duplicates('NR_RAS',keep='first')
	
	#Listando colunas repetidas da tabela auxiliar diferente de NR_RAS
	col_repetida=[]
	for coluna in concatenado.columns.to_list():
	    if coluna in esc.columns.to_list() and coluna!='NR_RAS':
	        col_repetida.append(coluna)
	
	print('Colunas repetidas:') #Resultados anteriores: ['DT_FECH_SINISTRO', 'VR_DESPESAS_SIN']
	print(col_repetida)
	#Excluindo colunas repetidas de concatenado
	concatenado.drop(col_repetida,axis=1,inplace=True)
	
	#Excluindo colunas indesejadas
	excluir=[]
	for coluna in concatenado.columns.to_list():
	  if coluna not in col_final:
	    excluir.append(coluna)
	concatenado=concatenado.drop(excluir, axis=1)
	concatenado
	
	#Capitalizando nomes
	concatenado['NM_GARANTIDO']=concatenado['NM_GARANTIDO'].str.title()
	
	#Tratando '..' na coluna NR_CELULAR_PORTO de concatenado
	concatenado['NR_CELULAR_PORTO']=concatenado['NR_CELULAR_PORTO'].apply(lambda x: '' if str(x)=='..' else str(x))
	
	concatenado['VR_COBERTURA_ALUGUEL']=concatenado['VR_COBERTURA_ALUGUEL'].astype(float)
	concatenado['VR_COBERTURA_ALUGUEL']=concatenado['VR_COBERTURA_ALUGUEL'].round(2)
	concatenado['VR_COBERTURA_ALUGUEL']=concatenado['VR_COBERTURA_ALUGUEL'].map('{:.2f}'.format)
	
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
	  arquivos=os.listdir(ingestion)
	  for arquivo in arquivos:
	    try:
	      os.rename(ingestion + arquivo, processedfiles + arquivo)
	    except:
	      os.remove(processedfiles + arquivo)
	      os.rename(ingestion + arquivo, processedfiles + arquivo)