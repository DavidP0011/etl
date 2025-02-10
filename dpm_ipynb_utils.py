def convert_notebook_to_script(notebook_name, notebook_folder):
    '''
    Convierte un Jupyter Notebook a un script de Python.

    Esta función toma el contenido de un notebook de Jupyter (archivo .ipynb)
    y lo convierte en un script de Python (archivo .py). Las celdas del notebook,
    incluyendo las celdas de código y las celdas de texto (markdown), se
    convertirán en código y comentarios en el script de Python, respectivamente.

    Parámetros:
    notebook_name (str): Nombre del notebook de Jupyter que se quiere convertir.
    notebook_folder (str): Ruta del directorio donde se encuentra el notebook y
                           donde se guardará el script resultante.

    Ejemplo de uso:
    convert_notebook_to_script('mi_notebook', '/ruta/a/mi/directorio')

    Nota:
    - Las celdas de markdown en el notebook se convertirán en comentarios en
      el script de Python, y las celdas de código se convertirán tal cual en código.
    - Si el archivo de salida ya existe, se sobrescribirá con el nuevo contenido.
    '''
    import os
    import nbformat
    from nbconvert import PythonExporter

    notebook_path = os.path.join(notebook_folder, notebook_name + '.ipynb')
    script_path = os.path.join(notebook_folder, notebook_name + '.py')

    # Cargar el notebook
    with open(notebook_path, 'r', encoding='utf-8') as fh:
        nb = nbformat.read(fh, as_version=4)

    # Convertir el notebook a script de Python
    exporter = PythonExporter()
    script_content, _ = exporter.from_notebook_node(nb)

    # Guardar el script
    with open(script_path, 'w', encoding='utf-8') as fh:
        fh.write(script_content)

    print(f'Conversión completada con éxito.\nNotebook convertido a: {script_path}')


def google_colab_GPU_RAM():
    '''
    Proporciona un resumen completo del entorno de ejecución en Google Colab,
    ayudando a entender y maximizar el uso de recursos disponibles. Esto incluye:

    - Información del sistema operativo y versión del kernel: Datos sobre el SO
      y el kernel de Linux utilizados por Colab.
    - Núcleos de CPU disponibles: Cantidad de núcleos de CPU accesibles para el
      procesamiento paralelo.
    - RAM disponible: Total de memoria RAM disponible y si se clasifica como un
      entorno de alta RAM.
    - Espacio de disco disponible: Espacio total y disponible en el disco para
      almacenamiento de datos.
    - Versión de TensorFlow: Versión actual de TensorFlow instalada, relevante
      para compatibilidad de código.
    - Verificación de GPU para TensorFlow: Detecta si TensorFlow puede acceder a
      una GPU y cuál.
    - Información sobre la GPU asignada: Detalles específicos sobre la GPU, si está
      disponible.
    - Versión de CUDA y cuDNN: Versiones de CUDA y cuDNN, cruciales para el
      entrenamiento acelerado en GPU.
    
    Ejecuta comandos de shell para recopilar y mostrar esta información, manejando
    errores de manera que no detenga la ejecución del notebook si se encuentran problemas
    con comandos específicos.
    '''

    import tensorflow as tf
    from psutil import virtual_memory
    import multiprocessing
    import os
    import subprocess

    def run_shell_command(cmd):
        '''Ejecuta un comando de shell y maneja la excepción si el comando falla.'''
        try:
            result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if result.returncode != 0:
                print(f"El comando '{cmd}' no se ejecutó correctamente: {result.stderr}")
                return ""
            return result.stdout
        except Exception as e:
            print(f"Error al intentar ejecutar el comando '{cmd}': {e}")
            return ""

    # Información del sistema
    print("Información del sistema operativo y versión del kernel:")
    print(run_shell_command("cat /etc/os-release"))
    print(run_shell_command("uname -a"))
    print('-'*80)

    # Información del CPU
    print(f"Núcleos de CPU disponibles: {multiprocessing.cpu_count()}")
    print('-'*80)

    # Información sobre la RAM disponible
    ram_gb = virtual_memory().total / 1e9
    if ram_gb > 20:
        print(f'RAM disponible del entorno de ejecución: {ram_gb:.1f} GB (alta RAM)')
    else:
        print(f'RAM disponible del entorno de ejecución: {ram_gb:.1f} GB')
    print('-'*80)

    # Espacio de disco disponible
    print("Espacio de disco disponible:")
    print(run_shell_command("df -h"))
    print('-'*80)

    # Versión de TensorFlow
    print(f'Versión de TensorFlow: {tf.__version__}')
    print('-'*80)

    # Verificación de GPU para TensorFlow
    if tf.test.gpu_device_name():
        print(f'TensorFlow está utilizando: {tf.test.gpu_device_name()}')
        # Información sobre la GPU asignada, solo si se detecta una GPU
        print("Información sobre la GPU asignada:")
        gpu_info = run_shell_command("nvidia-smi")
        if gpu_info:
            print(gpu_info)
        else:
            print("No se pudo obtener información de 'nvidia-smi'.")
    else:
        print("TensorFlow está utilizando la CPU")
    print('-'*80)

    # Versión de CUDA y cuDNN, solo ejecuta si se detecta una GPU
    if tf.test.gpu_device_name():
        print("Versión de CUDA y cuDNN:")
        print(run_shell_command("nvcc --version"))
        cudnn_info = run_shell_command("cat /usr/include/cudnn_version.h | grep CUDNN_MAJOR -A 2")
        if cudnn_info:
            print(cudnn_info)
        else:
            print("No se pudo obtener la versión de cuDNN.")
    else:
        print("No se detectó ninguna GPU. Saltando detalles de CUDA y cuDNN.")
    print('-'*80)


    # La lista de paquetes de Python instalados se puede habilitar si se necesita
    # print("Paquetes de Python instalados:")
    # print(run_shell_command("pip freeze"))
    # print('-'*80)
