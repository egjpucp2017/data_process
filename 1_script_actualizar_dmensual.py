
import datetime
import mysql.connector
import pandas as pd 
import matplotlib.pyplot as plt 
import pylab
import numpy as np
from datetime import datetime, date, timedelta
import FuncionesBd as fbd
import FuncionesBdInsertar as fbdIns



print ("Librerias Cargadas ")


def SetRegistro(path_excel) :
        cnx = mysql.connector.connect(user='root',
        database='andromeda',password="20102010")

        df = pd.read_csv(path_excel,sep=',',low_memory=False)
        #pd.read_csv('data.csv')
        #convirtiendo a mayusculas datrame
        df.columns = df.columns.str.lower()
        df.fillna(0)
        df.fillna("-.-")

        df_large = df.astype(object).where(pd.notnull(df), None)

        print ("contenido de dataframe")
        print (df_large)

        print ('--Informacion Acerca del DataFrame--')
        print (df_large.info())
        print ('--Fin acerca de informacion del Dataframe--')

        print("# de registros : "+str(df_large["pliego"].count()))

        #pliego[0:1]=='5'
        df2 = df_large[df_large['pliego'].astype(str).str[0:1]=='5']
        #df_filtered = df2[df2['pliego']=='510']

        #df_filtered = df2[df2['pliego'].astype(int)<600]

        #df_filtered =df2.pliego.str.contains('5')

        df2 = df2.astype(object).where(pd.notnull(df2), None)

        print("# de registros filtrados : "+str(df2["pliego"].count()))

        print(df2)

        print(df2.info())





        curDevengadoMensual = cnx.cursor(buffered=True)
        print ("Inciando Proceso de Actualizacion de Devengado Mensual")
        input("Presione Enter para continuar...")
        #2554341 to 2623483
        i=2554341
        while i < 2623483: #len(df2.index):
                                                                
                print("Actualizando registro = ",i)
                pliego=str(df2["pliego"][i]).strip()

                print("consulta_pliego = ",pliego)

                                                
                if pliego[0:1]=='5':
                        codigoPliego=str(pliego[0:3]).strip()
                        print("Procesando codigoPliego = ",codigoPliego)
                        periodo =2023
                        idpliego=fbd.GetBuscarPliegoPorCodigo(codigoPliego)
                        secejec= df2['sec_ejec'][i]
                        ejecutora=df2['ejecutora'][i]
                        ejecutoraNombre=df2['ejecutora_nombre'][i]
                        dptoEjecutora=df2['departamento_ejecutora'][i]
                        dptoEjecutoraNombre=df2['departamento_ejecutora_nombre'][i]	
	                
                        provEjecutora=df2['provincia_ejecutora'][i]
                        provEjecutoraNombre=df2['provincia_ejecutora_nombre'][i]
                        distEjecutora=df2['distrito_ejecutora'][i]
                        distEjecutoraNombre=df2['distrito_ejecutora_nombre'][i]
                        programaPpto=df2['programa_ppto'][i]
	                
                        programaPptoNombre=df2['programa_ppto_nombre'][i]
                        tipoActProy=df2['tipo_act_proy'][i]
                        productoProyecto=df2['producto_proyecto'][i]
                        productoProyectoNombre=df2['producto_proyecto_nombre'][i]
                        actividadAccionObra=df2['actividad_accion_obra'][i]
	                
                        actividadAccionObraNombre=df2['actividad_accion_obra_nombre'][i]
                        funcion=df2['funcion'][i]
                        funcionNombre=df2['funcion_nombre'][i]
                        divisionFuncional=df2['division_funcional'][i] 
                        divisionFuncionalNombre=df2['division_funcional_nombre'][i]
	                
                        grupoFuncional=df2['grupo_funcional'][i]  
                        grupoFuncionalNombre=df2['grupo_funcional_nombre'][i]
                        secFunc=df2['sec_func'][i]  
                        meta=df2['meta'][i] 
                        finalidad=df2['finalidad'][i]
	                
                        metaNombre=df2['meta_nombre'][i]
                        fuenteFinanciamiento=df2['fuente_financiamiento'][i]
                        fuenteFinanciamientoNombre=df2['fuente_financiamiento_nombre'][i]
                        rubro=df2['rubro'][i]  
                        rubroNombre=df2['rubro_nombre'][i]
	                
                        tipoRecurso=df2['tipo_recurso'][i]
                        tipoRecursoNombre=df2['tipo_recurso_nombre'][i]  
                        categoriaGasto=df2['categoria_gasto'][i]  
                        categoriaGastoNombre=df2['categoria_gasto_nombre'][i]  
                        tipoTransaccion=df2['tipo_transaccion'][i]
	                
                        tipoTransaccionNombre=df2['tipo_transaccion_nombre'][i]  
                        generica=df2['generica'][i]   
                        genericaNombre=df2['generica_nombre'][i]   
                        subGenerica=df2['subgenerica'][i]  
                        subGenericaNombre=df2['subgenerica_nombre'][i]
                        
                        subGenericaDet=df2['subgenerica_det'][i]
                        subGenericaDetNombre=df2['subgenerica_det_nombre'][i]

	                
                        especifica=df2['especifica'][i]  
                        especificaNombre=df2['especifica_nombre'][i]  
                        especificaDet=df2['especifica_det'][i]  
                        especificaDetNombre=df2['especifica_det_nombre'][i]  
                        pia=fbd.GetValor(df2["monto_pia"][i])
	                
                        pim=fbd.GetValor(df2["monto_pim"][i])  
                        certificado=fbd.GetValor(df2["monto_certificado_anual"][i])   
                        comprometido=fbd.GetValor(df2["monto_comprometido_anual"][i])    
                        devengado=fbd.GetValor(df2["monto_devengado_anual"][i])  
                        girado=fbd.GetValor(df2["monto_girado_anual"][i])	
                        devengadoEnero=fbd.GetValor(df2["monto_devengado_enero"][i])
                        devengadoFebrero=fbd.GetValor(df2["monto_devengado_febrero"][i])
                        devengadoMarzo=fbd.GetValor(df2["monto_devengado_marzo"][i])
                        devengadoAbril=fbd.GetValor(df2["monto_devengado_abril"][i])
                        devengadoMayo=fbd.GetValor(df2["monto_devengado_mayo"][i])
                        devengadoJunio=fbd.GetValor(df2["monto_devengado_junio"][i])
                        devengadoJulio=fbd.GetValor(df2["monto_devengado_julio"][i])
                        devengadoAgosto=fbd.GetValor(df2["monto_devengado_agosto"][i])
                        devengadoSetiembre=fbd.GetValor(df2["monto_devengado_septiembre"][i])
                        devengadoOctubre=fbd.GetValor(df2["monto_devengado_octubre"][i])
                        devengadoNoviembre=fbd.GetValor(df2["monto_devengado_noviembre"][i])
                        devengadoDiciembre=fbd.GetValor(df2["monto_devengado_diciembre"][i])



                        
                        actualizar_devengado_mensual=fbdIns.GetQueryInsertarDevengadoMensual(periodo,idpliego,
                        
                        secejec,ejecutora,ejecutoraNombre,dptoEjecutora,dptoEjecutoraNombre,                        
                        provEjecutora,provEjecutoraNombre,distEjecutora,distEjecutoraNombre,programaPpto,
                        programaPptoNombre,tipoActProy,productoProyecto,productoProyectoNombre,actividadAccionObra,
                        actividadAccionObraNombre,funcion,funcionNombre,divisionFuncional,divisionFuncionalNombre,
                        grupoFuncional,grupoFuncionalNombre,secFunc,meta,finalidad,

                        metaNombre,fuenteFinanciamiento,fuenteFinanciamientoNombre,rubro,rubroNombre,
                        tipoRecurso,tipoRecursoNombre,categoriaGasto,categoriaGastoNombre,tipoTransaccion,
                        tipoTransaccionNombre,generica,genericaNombre,subGenerica,subGenericaNombre,
                        especifica,especificaNombre,especificaDet,especificaDetNombre,
                        pia,pim,certificado,comprometido,devengado,
                        girado,devengadoEnero,devengadoFebrero,devengadoMarzo,devengadoAbril,
                        devengadoMayo,devengadoJunio,devengadoJulio,devengadoAgosto,devengadoSetiembre,
                        devengadoOctubre,devengadoNoviembre,devengadoDiciembre,
                        subGenericaDet,subGenericaDetNombre)

                        print(actualizar_devengado_mensual)
                                                        
                        curDevengadoMensual.execute(actualizar_devengado_mensual) 

                        cnx.commit()          
                                                        
                        print("Registro actualizado = ",pliego)
                                                        
                i=i+1
        cnx.close()
 
#create variable 
path_excel_origen='D:\\Users\\edwin\\dbox\\Dropbox\\ejecucion\\devengado_mensual\\'
path_archivo = '2023-Gasto-Devengado-Mensual.csv'
path_excel = path_excel_origen + path_archivo 
SetRegistro(path_excel)
                

print("Transaccion Completada")