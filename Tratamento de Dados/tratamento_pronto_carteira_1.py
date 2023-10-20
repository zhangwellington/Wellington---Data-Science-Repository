def tratamento():
	# **Tratamento PRONTO**
	
	## Inicialização
	
	mover=True
	
	import pandas as pd
	import numpy as np
	import os
	import datetime
	import time
	
	from google.colab import drive
	drive.mount('/content/drive')
	
	carteira='Pronto'
	
	#Definindo diretórios
	diretorio_tratamento=r'/content/drive/MyDrive/Processamentos/Tratamento/'
	diretorio=diretorio_tratamento + carteira + '/'
	output='/'.join(['/content/drive/MyDrive/Pronto pra upload',str(datetime.date.today().year),str(datetime.date.today().month).zfill(2),datetime.date.today().strftime('%d-%m-%Y')])+'/'
	os.makedirs(output,exist_ok=True)
	ingestion=diretorio+'ingestion/'
	processedfiles=diretorio+'processedfiles/'
	
	#Nome do arquivo de saída com caminho
	csv_saida=output+'early_pronto-carteira-1_'+str(datetime.date.today().strftime('%d-%m-%Y'))+'.csv'
	
	#Esperando ler pelo menos 1 arquivo na ingestion
	while len(os.listdir(ingestion))==0:
	  time.sleep(5)
	print(os.listdir(ingestion))
	
	#Identificando a planilha Asignacion
	for arquivo in os.listdir(ingestion):
	    if arquivo.find('Asignacion')>=0:
	        nome_asignacion=arquivo
	
	arquivo_asignacion=ingestion + nome_asignacion
	
	#Carregando Asignacion
	df_asignacion=pd.read_excel(arquivo_asignacion,dtype=object)
	
	## Início das Transformações
	
	### Apagar coluna de índice
	
	#Criando cópia do arquivo principal
	asignacion=df_asignacion.copy()
	print(asignacion.columns)
	print(asignacion.info())
	
	#Deletando coluna de índices
	del asignacion[asignacion.columns[0]]
	len(asignacion.columns)
	
	#Converter data pro formato brasileiro (não precisa)
	'''asignacion['vencimiento']=pd.to_datetime(asignacion['vencimiento'])
	asignacion['vencimiento']=asignacion['vencimiento'].dt.strftime('%d/%m/%Y')
	asignacion['vencimiento']'''
	
	### Adicionando as colunas: 'Moneda', 'Cuot minima deuda cliente' e 'Cantidad maxima cuotas' e reordenando as colunas
	
	#Adicionando colunas 'Moneda', 'Cuot minima deuda cliente' e 'Cantidad maxima cuotas' e reordenando as colunas
	asignacion['Moneda']='UYU'
	asignacion['Cuota minima deuda cliente']=200
	asignacion['Cantidad maxima cuotas']=1
	asignacion=pd.concat([asignacion.loc[:,:'CPostal'],asignacion.loc[:,'Moneda':'Cantidad maxima cuotas'],asignacion.loc[:,'Producto':'email']],join='outer',axis=1)
	asignacion.columns
	
	### Formatando 'DeudaCliente','DeudaMin' para 2 casas decimais
	
	#Formatando números para 2 casas decimais
	colunas=['DeudaCliente','DeudaMin']
	asignacion[colunas]=asignacion[colunas].astype(float)
	asignacion[colunas]=asignacion[colunas].round(2)
	for coluna in colunas:
	  asignacion[coluna]=asignacion[coluna].map('{:.2f}'.format)
	asignacion[colunas]
	
	### Retirando - e . da coluna Cedula
	
	#Retirando - e . da coluna Cedula
	asignacion['Cedula']=asignacion['Cedula'].str.replace('.','').str.replace('-','')
	asignacion['Cedula']
	
	###Renomeando as colunas
	
	asignacion.columns=['Cedula','Cliente','cartera','Apellido1','Apellido2','Nombre1','Nombre2',\
	                    'Edad','Sexo','CiuNom','DepNom','CPostal','Moneda','Cuota minima deuda cliente',\
	                    'Cantidad maxima cuotas','Producto','numero','Vencimiento','Deudacliente','Deudamin','Celular','email']
	
	###Tratamento de Email
	
	#Aplicando lower e strip na coluna de email
	coluna_email='email'
	asignacion[coluna_email]=asignacion[coluna_email].str.lower().str.strip()
	#Apagando emails não validados
	asignacion.loc[asignacion[coluna_email].str.find('@')<0,coluna_email]=''
	asignacion['num @']=asignacion[coluna_email].str.len()-asignacion[coluna_email].str.replace('@','').str.len()
	filtro=asignacion['num @']!=1
	asignacion.loc[filtro,coluna_email]=''
	asignacion.drop('num @',axis=1,inplace=True)
	
	###Tratamento do Celular (tirar 0 à esquerda)
	
	
	asignacion['Celular']=asignacion['Celular'].astype(int)
	
	###Tratar coluna de Nome
	
	coluna_nomes=['Apellido1','Apellido2','Nombre1','Nombre2']
	for coluna_nome in coluna_nomes:
	  asignacion[coluna_nome]=asignacion[coluna_nome].str.strip().str.title()
	
	## Exportando
	
	asignacion.to_csv(csv_saida,index=False)
	
	## Movendo arquivos processados
	
	if mover:
	  arquivos=[nome_asignacion]
	  for arquivo in arquivos:
	    try:
	      os.rename(ingestion + arquivo, processedfiles + arquivo)
	    except:
	      os.remove(processedfiles + arquivo)
	      os.rename(ingestion + arquivo, processedfiles + arquivo)