cd /home/franklin/ 

echo "Generando nùmero serial..." 
sudo dmidecode -t system | grep Serial > serial-number.txt

echo " " 
echo "Clonando repositorio..." 
echo " " 
sudo git clone https://github.com/Franklin-Wilber/proy-san-marcos.git
sudo sudo chmod -R 777 proy-san-marcos

cd /home/franklin/proy-san-marcos/

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