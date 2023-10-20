#Esse código envia todas as campanhas de email do dia pela AWS, sendo controlado por uma planilha externa


# ### Importar Bibliotecas

# In[49]:


from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
import datetime
import time
import os
import pandas as pd
import datetime


# In[84]:


segment_date=datetime.date.today()
dir_download=r'C:\Users\welli\Downloads\\'


# ### Tabela Auxiliar

# In[51]:


#Carregar tabela auxilair
tabela=pd.read_excel("D:\Desktop\Planilha Auxiliar - Selenium.xlsx",sheet_name='links')
rodar=pd.read_excel("D:\Desktop\Planilha Auxiliar - Selenium.xlsx",sheet_name='rodar')


# ### Abrir Navegador

# In[52]:


#Abrir firefox
driver = webdriver.Firefox()
driver.maximize_window()


# ### Login AWS

# In[53]:


#Carregar AWS
link=r'https://us-east-2.console.aws.amazon.com/'
driver.get(link)


# In[54]:


#Logar 1
user = driver.find_element(By.CSS_SELECTOR,'#resolving_input').send_keys('928120847291')
time.sleep(1)
proximo = driver.find_element(By.CSS_SELECTOR,'#next_button').click()


# In[55]:
user=''
pass=''

#Logar 2
username = driver.find_element(By.CSS_SELECTOR,'#username').send_keys(user)
password = driver.find_element(By.CSS_SELECTOR,'#password').send_keys(pass)
time.sleep(1)
sigin = driver.find_element(By.CSS_SELECTOR,'#signin_button').click()
time.sleep(5)


# ### Escolher Campanha

# In[56]:


print(tabela.loc[:,['Campanha']])


# In[57]:

for i in (1,2):
    today=datetime.date.today()+pd.Timedelta(days=i)
    for campanha in rodar.loc[rodar['Weekday']==i+4,'Campanha']:
        print('Início:',today,campanha)

        # ### All Projects

        # In[58]:


        #Carregar all projects
        time.sleep(5)
        link=r'https://us-east-2.console.aws.amazon.com/pinpoint/home?region=us-east-2#/apps'
        all_projects=driver.get(link)
        time.sleep(5)


        # In[59]:


        #Selecionar campanha
        selector=tabela.loc[tabela['Campanha']==campanha,'Project'].to_list()[0]
        project = driver.find_element(By.CSS_SELECTOR,selector).click()


        # ### Create Campaign

        # In[60]:


        #Campaigns
        campaigns=driver.find_element(By.CSS_SELECTOR,'.awsui_list-variant-root_l0dv0_dhz1x_155 > li:nth-child(6) > a:nth-child(1)').click()


        # In[61]:


        #Clicar em create campaign
        create_campaign=driver.find_element(By.CSS_SELECTOR,'.createCampaignButton > span:nth-child(1)').click()


        # In[62]:


        #Preencher My Campaign
        time.sleep(1)
        nome=today.strftime('%d.%m.%y')+' '+campanha
        my_campaign=driver.find_element(By.CSS_SELECTOR,"input[placeholder='My campaign']").send_keys(nome)


        # In[63]:


        #Clicar em Email
        email=driver.find_element(By.CSS_SELECTOR,"input[value='EMAIL']").click()


        # In[64]:


        #Next
        time.sleep(1)
        scroll_down=driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        next=driver.find_element(By.CSS_SELECTOR,".next-step-btn > span:nth-child(1)").click()


        # In[82]:


        #Choose segment
        choose_segment=driver.find_element(By.CSS_SELECTOR,".awsui_root_r2vco_d63m6_93").click()


        # In[87]:


        choose_segment2=driver.find_element(By.XPATH,"//span[contains(@title,'"+segment_date.strftime('%d.%m.%y')+' '+campanha+"')]").click()


        # In[88]:


        time.sleep(1)
        scroll_down=driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        next=driver.find_element(By.CSS_SELECTOR,".next-step-btn > span:nth-child(1)").click()


        # In[91]:


        #Escolher template
        template_number=str((today-datetime.date(2023,9,29)).days % 6) #Varia de 0 a 6 (escolhe template no excel)
        time.sleep(1)
        driver.find_element(By.CSS_SELECTOR,'.chooseTemplateButton > span:nth-child(1)').click()
        template=tabela.loc[tabela['Campanha']==campanha,'Template'+template_number].to_list()[0]
        time.sleep(1)
        select_template=driver.find_element(By.CSS_SELECTOR,"input[value='"+template+"']").click()
        choose_template=driver.find_element(By.CSS_SELECTOR,'span.awsui_root_18wu0_1tu1m_93 > div:nth-child(1) > div:nth-child(3) > button:nth-child(1)').click()


        # In[92]:


        #Next
        time.sleep(1)
        scroll_down=driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        next=driver.find_element(By.CSS_SELECTOR,'.next-step-btn > span:nth-child(1)').click()


        # In[93]:


        #Clicar em once
        driver.find_element(By.CSS_SELECTOR,'div.awsui_tile-container_vj6p7_1hgc8_259:nth-child(2) > div:nth-child(1) > span:nth-child(1)').click()


        # In[94]:


        #Preencher data e hora
        driver.find_element(By.CSS_SELECTOR,'.awsui_root_yodkx_im8v7_5 > input:nth-child(1)').send_keys(today.strftime('%Y/%m/%d'))


        # In[95]:


        driver.find_element(By.CSS_SELECTOR,'.awsui_root_l809c_im8v7_5 > input:nth-child(1)').send_keys('12:00')


        # In[96]:


        time.sleep(1)
        scroll_down=driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        next=driver.find_element(By.CSS_SELECTOR,'.next-step-btn > span:nth-child(1)').click()


        # In[97]:


        #Launch Campaign
        time.sleep(1)
        scroll_down=driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
        launch_campaign=driver.find_element(By.CSS_SELECTOR,'.launchCampaign > span:nth-child(1)').click()
        
        #Print
        print('Finalizado:',today,campanha)
print('Todas campanhas programadas com sucesso!!!')
driver.close()

