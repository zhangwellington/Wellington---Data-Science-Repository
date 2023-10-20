def tratamento():
	# **Tratamento NATURA PE EXCLUSIVA**
	
	## Inicialização
	
	tem_especial=True
	mover=True
	
	import pandas as pd
	import numpy as np
	import os
	import datetime
	import time
	
	from google.colab import drive
	drive.mount('/content/drive')
	
	carteira='Natura PE'
	
	#Definindo diretórios
	diretorio_tratamento=r'/content/drive/MyDrive/Processamentos/Tratamento/'
	diretorio=diretorio_tratamento + carteira + '/'
	output='/'.join(['/content/drive/MyDrive/Pronto pra upload',str(datetime.date.today().year),str(datetime.date.today().month).zfill(2),datetime.date.today().strftime('%d-%m-%Y')])+'/'
	os.makedirs(output,exist_ok=True)
	ingestion=diretorio+'ingestion/'
	planton=diretorio+'Plantón/'
	processedfiles=diretorio+'processedfiles/'
	dir_especial=diretorio+'Especial/'
	dir_num_errados=diretorio+'Números Inválidos/'
	os.makedirs(dir_num_errados,exist_ok=True)
	
	today=datetime.date.today()
	#Nome do arquivo de saída com caminho
	csv_saida=output+'early_natura-peru-exclusiva_'+str(today.strftime('%d-%m-%Y'))+'.csv'
	#Arquivo com números inválidos
	errados_saida=dir_num_errados+'numeros-errados_natura-pe-exclusiva_'+str(today)+'.xlsx'
	
	#Esperando ler 1
	num_arquivo=1
	
	while len(os.listdir(ingestion))!=num_arquivo:
	  time.sleep(5)
	print(os.listdir(ingestion))
	
	#Identificando as planilhas EXCLUSIVA e ESPECIAL
	for arquivo in os.listdir(ingestion):
	    if arquivo.find('ASIG')>=0:
	      nome_exclusiva=arquivo
	      arquivo_exclusiva=ingestion+arquivo
	
	#Identificando o arquivo mais recente da pasta Exclusiva
	if tem_especial:
	  df_arquivo_recente=pd.DataFrame({'Nome':os.listdir(dir_especial)})
	  tempo_modif=np.empty(len(df_arquivo_recente))
	  i=0
	  for arquivo in os.listdir(dir_especial):
	    tempo_modif[i]=os.path.getmtime(dir_especial+arquivo)
	    i+=1
	  df_arquivo_recente['Tempo de Modificacao']=tempo_modif
	  df_arquivo_recente=df_arquivo_recente.sort_values(by='Tempo de Modificacao',ascending=False)
	  df_arquivo_recente.index=range(len(df_arquivo_recente))
	  print(df_arquivo_recente)
	  nome_arquivo_especial=str(df_arquivo_recente['Nome'][0])
	  arquivo_especial=dir_especial+nome_arquivo_especial
	  print(arquivo_especial)
	
	#Identificando o arquivo mais recente da pasta Plantón
	df_arquivo_recente=pd.DataFrame({'Nome':os.listdir(planton)})
	tempo_modif=np.empty(len(df_arquivo_recente))
	i=0
	for arquivo in os.listdir(planton):
	  tempo_modif[i]=os.path.getmtime(planton+arquivo)
	  i+=1
	df_arquivo_recente['Tempo de Modificacao']=tempo_modif
	df_arquivo_recente=df_arquivo_recente.sort_values(by='Tempo de Modificacao',ascending=False)
	df_arquivo_recente.index=range(len(df_arquivo_recente))
	print(df_arquivo_recente)
	nome_arquivo_platon=str(df_arquivo_recente['Nome'][0])
	arquivo_planton=planton+nome_arquivo_platon
	print(arquivo_planton)
	
	#csv_saida2=arquivo_exclusiva.replace('.xlsx',' - Tratado com Planilha.csv').replace(ingestion,output)
	#xlsx_saida=arquivo_exclusiva.replace('.xlsx',' - Tratado.xlsx').replace(ingestion,output)
	
	if tem_especial:
	    print(arquivo_exclusiva,'\n',arquivo_especial,'\n',arquivo_planton)
	else:
	    print(arquivo_exclusiva,'\n',arquivo_planton)
	
	#Carregando Planilhas
	exclusiva=pd.read_excel(arquivo_exclusiva,dtype=object)
	exclusiva['Planilha']='EXCLUSIVA'
	print('Exclusiva carregado com sucesso!')
	df_planton=pd.read_excel(arquivo_planton,dtype=object)
	print('Plantón carregado com sucesso!')
	if tem_especial:
	    especial=pd.read_excel(arquivo_especial,dtype=object)
	    print('Especial carregado com sucesso!')
	    especial['Planilha']='ESPECIAL'
	else:
	    print('Carteira Especial não existente')
	
	## Início das Transformações
	
	### Concatenar Exclusiva e Especial
	
	#Concatenando Carteiras e criando cópias
	if tem_especial:
	    concatenar=pd.concat([exclusiva,especial],join='outer',ignore_index=True)
	    cartera=concatenar.copy()
	else:
	    cartera=exclusiva.copy()
	planton=df_planton.copy()
	
	###Filtrar descontos da Planton por data
	
	planton['Fecha Vigencia'] = pd.to_datetime(planton['Fecha Vigencia'])
	filtro=planton['Fecha Vigencia']>=pd.Timestamp(today)
	planton=planton[filtro]
	
	### Capitalizar 'Nombre de la Persona'
	
	#Capitalizando coluna de Nomes
	cartera['Nombre de la Persona']=cartera['Nombre de la Persona'].str.title()
	cartera['Nombre de la Persona']
	
	### Tratando '% Descuento Saldo Original' para ficar que nem no excel em %
	
	#Colocando % na coluna de Descontos e multiplicando por 100
	cartera['% Descuento Saldo Original']=cartera['% Descuento Saldo Original'].astype(float)*100
	cartera['% Descuento Saldo Original']=cartera['% Descuento Saldo Original'].astype(int)
	cartera['% Descuento Saldo Original']=cartera['% Descuento Saldo Original'].apply(lambda x: str(x)+'%')
	cartera['% Descuento Saldo Original']
	
	### Tratando datas da coluna 'Fecha nasc'
	
	#Tratando a coluna de Fecha Nasc que tem datas em formato de número
	coluna='Fecha nasc'
	# cartera[coluna]=cartera[coluna].apply(lambda x:'' if str(x)=='-' else x)
	data=np.array(cartera[coluna])
	for i in range(len(data)):
	  if str(data[i]).find('/')==-1 and str(data[i]).find('-')==-1:
	      data[i]=str(pd.to_datetime('1900-01-01')+pd.Timedelta(days=data[i]-2))
	cartera[coluna]=data
	cartera[coluna]=cartera[coluna].apply(lambda x: str(x)[:10])
	
	#Tratando as datas que estão com - para /
	x=np.array(cartera[coluna])
	print(x)
	for i in range(len(x)):
	  if x[i].find('/')==-1:
	    x[i]=str(x[i])[-2:]+'/'+str(x[i])[5:7]+'/'+str(x[i])[:4]
	cartera[coluna]=x
	#Datas que são - viram -//- depois das operações acima, substituir essas datas por vazio
	cartera[coluna]=cartera[coluna].str.replace('-//-','')
	cartera[coluna]
	
	### Tirando o horário das colunas de data e convertendo pro formato brasileiro
	
	#Tirando horário nas colunas de data
	print(cartera.select_dtypes(include='datetime').columns.tolist())
	for coluna in cartera.select_dtypes(include='datetime').columns.tolist():
	    cartera[coluna]=cartera[coluna].apply(lambda x: str(x)[:10])
	    cartera[coluna]=pd.to_datetime(cartera[coluna])
	    cartera[coluna]=cartera[coluna].dt.strftime('%d/%m/%Y')
	
	### Remover colunas 'Estructura Comercial','Estructura Comercial Padre'
	
	#Apagando colunas de Estrutuctura Comercial
	cartera=cartera.drop(['Estructura Comercial','Estructura Comercial Padre'],axis=1)
	
	### Remover DNI nulos, zeros e - e preencher com 8 dígitos
	
	#Tratando DNI nulo e hífens
	print('Total de linhas antes:',len(cartera))
	nulos=np.array(cartera['DNI'].notnull())
	hifens=np.array(cartera['DNI']!='-')
	filtro=nulos*hifens
	print('Total de linhas diferente de - e nulo:',filtro.sum())
	cartera=cartera[filtro]
	#Formatando DNI com 8 dígitos
	cartera['DNI']=cartera['DNI'].astype(object)
	cartera['DNI']=cartera['DNI'].apply(lambda x: str(x).zfill(8))
	#Removendo zeros
	cartera=cartera[cartera['DNI']!='00000000']
	print('Total de linhas depois de filtrar zeros:',len(cartera))
	cartera.sort_values('DNI').head(3)
	
	### Adicionando coluna 'MONEDA'
	
	#Criando coluna MONEDA
	cartera['MONEDA']='PEN'
	
	### Mesclar tabela Plantón
	
	#Mesclando tabelas
	codigo_importe=nome_arquivo_platon[13:16] #12:15 no script local
	importe_a_pagar='Importe a pagar - Plantón Cobranza ' + codigo_importe     #'Importe a pagar - Plantón Cobranza ' + codigo_importe
	planton=planton[['No Pedido',importe_a_pagar]]
	#planton.columns=['No Pedido',importe_a_pagar]   #Renomear coluna de Saldo(Antigo Importe a pagar) porque já tem coluna Saldo na carteira
	mesclado=cartera.merge(planton,on='No Pedido',how='left')
	
	### Substituindo Monto Mínimo a Pagar pelo valor do Platón
	
	#Substituindo Monto Mínimo a Pagar pelo valor do Platón
	importe_nao_nulo=mesclado[importe_a_pagar].notnull()
	importe_nao_nulo.sum()
	valores=np.array(mesclado['Monto mínimo a pagar Campaña'])
	valores[importe_nao_nulo]=mesclado[importe_nao_nulo][importe_a_pagar]
	mesclado['Monto mínimo a pagar Campaña']=valores
	
	###Criar coluna de Descuentos
	
	
	mesclado['Descuentos'] = 0
	mesclado.loc[importe_nao_nulo,'Descuentos']=1
	
	### Apagar a coluna auxiliar 'Importe a pagar - Plantón Cobranza C10'
	
	mesclado=mesclado.drop(importe_a_pagar,axis=1)
	
	### Remover 'No Pedido' duplicados
	
	#Remover Pedidos duplicados
	print(len(mesclado))
	mesclado=mesclado.drop_duplicates(subset='No Pedido')
	print(len(mesclado))
	
	### Arredondando e formatando colunas de números para 2 casas decimais
	
	#Formatar números para 2 casas decimais
	colunas=['Original', 'Saldo','Saldo Corregido','Monto mínimo a pagar Campaña','Interés/multa Acumulados (100% descuento)','Monto Descuento Saldo Original','Total descuentos']
	for coluna in colunas:
	    mesclado[coluna]=mesclado[coluna].astype(float)
	    mesclado[coluna]=mesclado[coluna].round(2)
	    mesclado[coluna]=mesclado[coluna].map('{:.2f}'.format)
	
	###Tratamento dos números de telefone
	
	#Fazendo coerce com Celular e Telefone
	telefone=np.array(mesclado['Teléfono Persona'])
	celular=np.array(mesclado['Numero Celular'])
	celular_nao_nulo=mesclado['Numero Celular'].notnull()
	telefone[celular_nao_nulo]=celular[celular_nao_nulo]
	mesclado['Numero Celular']=telefone
	#Tirando espaços
	mesclado['Numero Celular']=mesclado['Numero Celular'].astype(str).str.strip()
	#Tratando números com mais de 9 dígitos e 8 dígitos
	mesclado['Numero Celular']=mesclado['Numero Celular'].apply(lambda x: '9'+str(x) if len(str(x))==8 else (str(x)[-9:] if len(str(x))>9 else x))
	#Apagar números que não começam com 9
	mesclado['Numero Celular']=mesclado['Numero Celular'].apply(lambda x: '' if str(x)[0]!='9' else x)
	#Apagar números com menos de 8 dígtios
	filtro=np.char.str_len(np.char.array(mesclado['Numero Celular']))<8
	#Exportar os números errados
	mesclado.loc[:,'Numero Celular'][filtro].to_csv(errados_saida,index=False)
	#Substituir por '' os números errados
	mesclado.loc[:,'Numero Celular'][filtro]=''
	
	###Tratamento de Email
	
	# Aplicando lower e strip na coluna de email
	coluna_email = 'Correo Electrónico'
	mesclado[coluna_email] = mesclado[coluna_email].str.lower().str.strip()
	# Apagando emails não validados
	mesclado[coluna_email].loc[mesclado[coluna_email].str.find('@') < 0] = ''
	mesclado['num @'] = mesclado[coluna_email].str.len() - mesclado[coluna_email].str.replace('@', '').str.len()
	filtro = mesclado['num @'] != 1
	mesclado.loc[filtro, coluna_email] = ''
	mesclado.drop('num @', axis=1, inplace=True)
	
	###Tratar coluna de Nome
	
	coluna_nomes=['Nombre de la Persona']
	for coluna_nome in coluna_nomes:
	  mesclado[coluna_nome]=mesclado[coluna_nome].str.strip().str.title()
	
	
	
	## Exportando
	
	#mesclado.to_csv(csv_saida2,index=False)
	try:
	  mesclado=mesclado.drop('Planilha',axis=1)
	except:
	  print('Coluna planilha já excluída')
	#mesclado.to_excel(xlsx_saida,index=False)
	mesclado.to_csv(csv_saida,index=False)
	
	## Movendo Somente as Carteiras Exclusivas
	
	if mover:
	  arquivos=[nome_exclusiva]
	  for arquivo in arquivos:
	    try:
	      os.rename(ingestion + arquivo, processedfiles + arquivo)
	    except:
	      os.remove(processedfiles + arquivo)
	      os.rename(ingestion + arquivo, processedfiles + arquivo)