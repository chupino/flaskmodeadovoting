from flask import Flask, request, render_template
from kafka import KafkaProducer
import time

app = Flask(__name__)

producer = KafkaProducer(bootstrap_servers='54.161.135.240:9093')

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        start_time = time.time()
        # Obtener el archivo del formulario
        file = request.files['file']
        
        if file:
            lines_to_send = []
            # Leer el archivo línea por línea
            for line in file:
                line_decoded = line.decode('utf-8', errors='ignore').strip()
                lines_to_send.append(line_decoded)
                # Enviar cada 100 líneas (ajusta este número según sea necesario)
                if len(lines_to_send) >= 100:
                    # Enviar a Kafka sin llamar a flush
                    producer.send('bigdata', value='\n'.join(lines_to_send).encode('utf-8'))
                    lines_to_send = []  # Reiniciar el acumulador
            # Enviar cualquier línea restante
            if lines_to_send:
                producer.send('bigdata', value='\n'.join(lines_to_send).encode('utf-8'))

            end_time = time.time()
            total_time = end_time - start_time
            return f'Archivo procesado en {total_time:.2f} segundos. Revisa la consola para ver las líneas enviadas a Kafka.'

    return render_template('upload_form.html')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=80)  # Configurado para Docker