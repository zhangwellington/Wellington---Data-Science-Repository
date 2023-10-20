def tratamento():
	# **Tratamento NATURA MEX PADRÃO**
	
	mover=True
	
	import pandas as pd
	import numpy as np
	import os
	import time
	import datetime
	
	from google.colab import drive
	drive.mount('/content/drive')
	
	##Definindo Diretórios
	
	carteira='Natura MEX'
	
	#Definindo diretórios
	diretorio_tratamento=r'/content/drive/MyDrive/Processamentos/Tratamento/'
	diretorio=diretorio_tratamento + carteira + '/'
	output='/'.join(['/content/drive/MyDrive/Pronto pra upload',str(datetime.date.today().year),str(datetime.date.today().month).zfill(2),datetime.date.today().strftime('%d-%m-%Y')])+'/'
	os.makedirs(output,exist_ok=True)
	ingestion=diretorio+'ingestion/'
	processedfiles=diretorio+'processedfiles/'
	auxiliary=diretorio+'auxiliary/'
	dir_num_errados=diretorio+'Números Inválidos/'
	os.makedirs(dir_num_errados,exist_ok=True)
	#Nome do arquivo de saída com caminho
	today=datetime.date.today()
	csv_saida=output+'early_natura-mex-padrao_'+str(today.strftime('%d-%m-%Y'))+'.csv'
	#Arquivo com números inválidos
	errados_saida=dir_num_errados+'numeros-errados_natura-mex-padrao_'+str(today)+'.xlsx'
	#Checando arquivos na ingestion
	print(os.listdir(ingestion))
	
	#Mapeando o arquivo Rep
	nome_rep_cartera=''
	while nome_rep_cartera=='':
	  for arquivo in os.listdir(ingestion):
	    if arquivo[:3]=='Rep':
	      nome_rep_cartera=arquivo
	  time.sleep(5)
	
	concatenado=pd.read_csv(auxiliary+'Personas Concatenado.csv',dtype=object)
	concatenado
	
	df_rep_cartera=pd.read_csv(ingestion+nome_rep_cartera,dtype=object,encoding='utf_8')
	
	##Cópia da carteira principal
	
	rep_cartera=df_rep_cartera.copy()
	
	##Início das Transformações
	
	#Capitalizando coluna de nomes
	rep_cartera['Nombre']=rep_cartera['Nombre'].str.title()
	
	#Mesclando Rep_Cartera
	rep_cartera_mesclado=rep_cartera.merge(concatenado, left_on='Codigo Persona',right_on='CN',how='left')
	rep_cartera_mesclado=rep_cartera_mesclado.drop('CN',axis=1)
	
	#Número de vazios nas duas carteiras
	#print('Número de vazios em Cartera: '+str(cartera_mesclado[r"'IFE/RFC'"].isnull().sum()))
	print('Número de vazios em RepCartera: '+str(rep_cartera_mesclado[r"'IFE/RFC'"].isnull().sum()))
	
	#Exportando os Nulos
	#rep_cartera_mesclado[rep_cartera_mesclado[r"'IFE/RFC'"].isnull()].to_excel(output + nome_saida_rep_nulos,index=False)
	
	rep_cartera_mesclado.columns
	
	#Formatando colunas numéricas para 2 casas decimais
	colunas=['Valor Titulo', 'Saldo Principal','Saldo actualizado']
	rep_cartera_mesclado[colunas]=rep_cartera_mesclado[colunas].astype(float)
	for coluna in colunas:
	  rep_cartera_mesclado[coluna]=rep_cartera_mesclado[coluna].map('{:.2f}'.format)
	rep_cartera_mesclado[colunas]
	
	#Apagar IFE nulos
	rep_cartera_nao_nulos=rep_cartera_mesclado.dropna(subset=r"'IFE/RFC'")
	
	#Apagar coluna Dias en la instancia actual
	rep_cartera_nao_nulos=rep_cartera_nao_nulos.drop('Dias en la instancia actual',axis=1)
	
	###Tratando número de telefones
	
	celular=np.array(rep_cartera_nao_nulos['Telefono movil']).astype(str)
	for i in range(len(celular)):
	  if len(celular[i])>10:
	    celular[i]=str(celular[i])[-10:]
	  elif len(celular[i])<10 or celular[i]==''.zfill(10):
	    celular[i]=''
	
	#Substituindo números pelos valores da lista
	rep_cartera_nao_nulos.loc[:,'Telefono movil']=celular
	
	###Tratamento de Email
	
	###Tratamento de Email
	# Aplicando lower e strip na coluna de email
	coluna_email = 'Correo electronico personal'
	rep_cartera_nao_nulos[coluna_email] = rep_cartera_nao_nulos[coluna_email].str.lower().str.strip()
	# Apagando emails não validados
	rep_cartera_nao_nulos[coluna_email].loc[rep_cartera_nao_nulos[coluna_email].str.find('@') < 0] = ''
	rep_cartera_nao_nulos['num @'] = rep_cartera_nao_nulos[coluna_email].str.len() - rep_cartera_nao_nulos[coluna_email].str.replace('@', '').str.len()
	filtro = rep_cartera_nao_nulos['num @'] != 1
	rep_cartera_nao_nulos.loc[filtro, coluna_email] = ''
	rep_cartera_nao_nulos.drop('num @', axis=1, inplace=True)
	
	###Renomear colunas
	
	#Renomeando as colunas
	rep_cartera_nao_nulos.columns=['INDICE','CódigoPersona','Nombre','NúmeroPedido','FechaVencimiento','Dias de credito','Dias de morosidad',\
	                               'Temporalidad Agencias','ValorTítulo','Saldo Principal','Saldo actualizado','Estructura Padre','Estructura',\
	                               'Direccion Completa','Telefono movil','Telefono Residencial','Correo electronico personal','Referencia Bancaria',\
	                               'Agencia Asignada MC','Estatus MC','curp']
	
	###Tratar coluna de Nome
	
	coluna_nomes=['Nombre']
	for coluna_nome in coluna_nomes:
	  rep_cartera_nao_nulos[coluna_nome]=rep_cartera_nao_nulos[coluna_nome].str.strip().str.title()
	
	##Exportando
	
	###Exportando arquivos de números errados
	
	#Criando cópia
	num_errados=df_rep_cartera.copy()
	#Pegando lista de números
	numeros=np.array(num_errados['Telefono movil']).astype(str)
	#Criando filtro
	length=np.char.str_len(numeros)
	filtro1=length<10
	filtro2=numeros==''.zfill(10)
	filtro=filtro1+filtro2
	print(filtro1.sum(),filtro2.sum(),filtro.sum(),len(filtro))
	#Aplicando filtro
	num_errados=num_errados[filtro]
	#Exportando arquivo
	num_errados.to_excel(errados_saida,index=False)
	
	###Exportando arquivo tratado
	
	#Exportando arquivo tratado
	rep_cartera_nao_nulos.to_csv(csv_saida,index=False)
	
	##Movendo arquivos
	
	#Movendo arquivos para processedfiles
	if mover:
	  arquivos=[nome_rep_cartera]
	  for arquivo in arquivos:
	    try:
	      os.rename(ingestion + arquivo, processedfiles + arquivo)
	    except:
	      os.remove(processedfiles + arquivo)
	      os.rename(ingestion + arquivo, processedfiles + arquivo)