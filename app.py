from flask import Flask, request, render_template
from kafka import KafkaProducer
import time

app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers='18.207.186.163:9093')

# HTML simple para subir archivos
html_form = """
<!DOCTYPE html>
<html>
<head>
    <title>Subir archivo</title>
</head>
<body>
    <h1>Subir un archivo .data</h1>
    <form method="POST" enctype="multipart/form-data">
        <input type="file" name="file" />
        <input type="submit" value="Subir" />
    </form>
</body>
</html>
"""

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        start_time = time.time()
        # Obtener el archivo del formulario
        file = request.files['file']
        
        if file:
            # Leer el archivo línea por línea
            for line in file:
                line_decoded = line.decode('utf-8', errors='ignore').strip()
                producer.send('bigdata', value=line_decoded.encode('utf-8'))
                print(line_decoded)
            end_time = time.time()
            total_time = end_time - start_time
            return f'Archivo procesado en {total_time:.2f}. Revisa la consola para ver las líneas.'

    return render_template('upload_form.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80)  # Configurado para Docker