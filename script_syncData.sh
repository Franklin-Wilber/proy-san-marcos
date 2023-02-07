#!/bin/bash
sudo echo "Instalando Git..."
sudo apt-get update
sudo apt-get -yq install git
sudo git config --global user.name "Franklin"
sudo git config --global user.email "frank.vasquez.obregon@gmail.com"

sudo echo ""
sudo echo "Python PIP..."
sudo apt-get install -y python3-pip --yes

sudo echo ""
sudo echo "Python Pandas..."
sudo apt install python3-pandas --yes

sudo echo ""
sudo echo "Clonando repositorio..." 
cd /home/$USER/
sudo git clone https://github.com/Franklin-Wilber/proy-san-marcos.git
sudo sudo chmod -R 777 /home/$USER/proy-san-marcos/

cd proy-san-marcos

sudo echo ""
echo "Generando nùmero serial..." 
sudo dmidecode -t system | grep Serial > serial-number.txt

echo " " 
echo "Instalando dependencias..." 
echo " " 
pip install google-cloud-pubsub

echo " " 
echo "Creando BD..." 
echo " " 
sudo python3 execute_cmd.py --action create-database

echo " " 
echo "Creando subscruptions..." 
echo " " 
sudo python3 execute_cmd.py --action create-subscription

echo " " 
echo "Creando cursos..." 
echo " " 
sudo python3 execute_cmd.py --action sync-courses

echo " " 
echo "Creando registrando alumnos..." 
echo " " 
sudo python3 execute_cmd.py --action sync-people-students

echo " " 
echo "Creando registrando profesores..." 
echo " " 
sudo python3 execute_cmd.py --action sync-people-teachers
