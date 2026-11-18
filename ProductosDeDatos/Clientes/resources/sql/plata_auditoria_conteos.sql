SELECT (SELECT count(*) FROM {catalogo}.bronce.clientes)                                                    AS FilasBronce
     , (SELECT count(*) FROM {catalogo}.plata.clientes)                                                     AS FilasPlata
     , (SELECT count(*) FROM {catalogo}.bronce.clientes) - (SELECT count(*) FROM {catalogo}.plata.clientes) AS Descartadas;