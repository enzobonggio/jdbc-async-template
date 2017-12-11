package com.async.jdbc;

import com.async.jdbc.impl.JdbcAsyncTemplate;
import com.github.pgasync.ResultSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;

public class JdbcTemplateTestSpec {

    private JdbcAsyncOperations op;

    @Before
    public void setupBefore() {
        op = new JdbcAsyncTemplate(TestCommon.getConnectionPool(), TestCommon.getConvertStrategy());

    }

    @Test
    public void queryWithParams() {
        ResultSet row = TestCommon
                .getConnectionPool().querySet("SELECT * FROM admmcli.mcli_cliente WHERE id_cliente=$1", 1)
                .toBlocking().first();//(rows -> System.out.println("rows" + rows.size()));
        System.out.println("Real time per process: ");

    }

    @Test
    public void queryForObjects() {
        long currentMilliseconds = System.currentTimeMillis();
        List<TestVO> result = op.queryForObjects(
                "SELECT * FROM admmcli.mcli_cliente",
                new HashMap<>(),
                TestVO.class)
                .collectList().block();
        long doneTime = System.currentTimeMillis();

        double realTime = (doneTime - currentMilliseconds);
        System.out.println("Real time per process: " + realTime + " " + result.size());
    }

    @Test
    public void pagedQuery() {
        List<TestVO> page = op.queryForPagedObjects(
                "SELECT * FROM admmcli.mcli_cliente",
                new HashMap<>(),
                TestVO.class,
                0,
                10)
                .block().getContent().collectList().block();

        Assert.assertTrue(page.size() == 10);
    }

    @Test
    public void heavyQuery() {
        HashMap<String, Object> params = new HashMap<String, Object>();
        params.put("idCliente", 53770);
        List<TrxVo> trxs = op.queryForObjects(
                "SELECT * FROM (SELECT cod_mtt.id id, cod_mtt.marca marca, cod_mtt.cod_familia cod_familia, cod_mtt.familia familia, cod_mtt.fecha_trx fecha_trx, cod_mtt.fecha_posteo fecha_posteo, cod_mtt.tipo_titularidad tipo_titularidad, cod_mtt.tarjeta tarjeta, cod_mtt.tipo_trx tipo_trx, NULL :: VARCHAR estado, cod_mtt.comercio comercio, cod_mtt.origen origen, cod_mtt.monto_trx monto_trx, cod_mtt.monto_pie, cod_mtt.monto_total monto_total, cod_mtt.clave :: INT :: BOOL clave, TRUE trx_relacionada, cod_mtt.otorgamiento otorgamiento, cod_mtt.cuotas cuotas, cod_mtt.valor_cuota :: DOUBLE PRECISION valor_cuota, cod_mtt.diferido :: DOUBLE PRECISION diferido, cod_mtt.tasa_interes :: DOUBLE PRECISION tasa_interes, cod_mtt.codigo_autorizacion codigo_autorizacion, (SELECT trx.fecha_transaccion FROM admmcta.mcta_transaccion_tarjeta trx INNER JOIN admmref.mref_grupo_transaccion gr ON gr.id = trx.id_grupo_trx WHERE trx.id_tarjeta = mtt.id_tarjeta AND trx.codigo_autorizacion = mtt.codigo_autorizacion AND gr.codigo IN ('30', '180', '3000')) fecha_ord FROM admmcta.mcta_transaccion_tarjeta mtt INNER JOIN admmcta.mcta_tipo_transaccion_tarjeta mttt ON mttt.id = mtt.id_tipo_transaccion_tj INNER JOIN admmref.mref_grupo_transaccion mgt ON mgt.id = mtt.id_grupo_trx INNER JOIN admmcta.mcta_maestro_tarjeta mmt ON mtt.id_tarjeta = mmt.id_tarjeta INNER JOIN admmcta.mcta_marca_tarjeta mmarcat ON mmarcat.id = mmt.id_marca_tarjeta INNER JOIN admmref.mref_logo mrlogo ON mrlogo.id = mmarcat.id_logo RIGHT JOIN (SELECT in_mmt.id_cliente, in_mtt.id_transaccion_tj id, in_mrlogo.descripcion_corta marca, in_mgt.codigo cod_familia, in_mgt.descripcion_corta :: VARCHAR familia, in_mtt.fecha_transaccion fecha_trx, in_mtt.fecha_posteo fecha_posteo, in_mtrc.descripcion_corta tipo_titularidad, in_mmt.numero_tarjeta tarjeta, in_mttt.nombre_transaccion tipo_trx, NULL :: VARCHAR estado, in_mtt.descripcion_local comercio, in_mttt.tipo_transaccion origen, in_mtt.monto_transaccion monto_trx, in_mtt.monto_pie, (CASE WHEN in_mtt.numero_cuotas <> 0 THEN in_mtt.valor_cuota * in_mtt.numero_cuotas ELSE in_mtt.monto_transaccion END) monto_total, in_mtt.uso_de_clave :: INT :: BOOL clave, in_mtt.otorgamiento otorgamiento, in_mtt.numero_cuotas cuotas, in_mtt.valor_cuota valor_cuota, in_mtt.meses_diferimiento diferido, in_mtt.tasa_interes_cuotas tasa_interes, in_mtt.codigo_autorizacion codigo_autorizacion FROM admmcta.mcta_transaccion_tarjeta in_mtt INNER JOIN admmref.mref_grupo_transaccion in_mgt ON in_mgt.id = in_mtt.id_grupo_trx INNER JOIN admmcta.mcta_tipo_transaccion_tarjeta in_mttt ON in_mttt.id = in_mtt.id_tipo_transaccion_tj INNER JOIN admmcta.mcta_maestro_tarjeta in_mmt ON in_mtt.id_tarjeta = in_mmt.id_tarjeta INNER JOIN admmcta.mcta_marca_tarjeta in_mmarcat ON in_mmarcat.id = in_mmt.id_marca_tarjeta INNER JOIN admmref.mref_logo in_mrlogo ON in_mrlogo.id = in_mmarcat.id_logo INNER JOIN admmref.mref_tipo_rol_cliente in_mtrc ON in_mtrc.id = in_mmt.id_tipo_tarjeta) cod_mtt ON mtt.codigo_autorizacion = cod_mtt.codigo_autorizacion AND cod_mtt.id_cliente = mmt.id_cliente WHERE 1 = 1 AND mttt.registro_seleccionable = 1 AND mmt.id_cliente = :idCliente AND mtt.fecha_transaccion >= NOW() - INTERVAL '25' MONTH AND mtt.codigo_autorizacion != '0' UNION SELECT mtt.id_transaccion_tj id, mrlogo.descripcion_corta marca, mgt.codigo cod_familia, mgt.descripcion_corta :: VARCHAR familia, mtt.fecha_transaccion fecha_trx, mtt.fecha_posteo fecha_posteo, mtrc.descripcion_corta tipo_titularidad, mmt.numero_tarjeta tarjeta, mttt.nombre_transaccion tipo_trx, NULL :: VARCHAR estado, mtt.descripcion_local comercio, mttt.tipo_transaccion origen, mtt.monto_transaccion monto_trx, mtt.monto_pie, (CASE WHEN mtt.numero_cuotas <> 0 THEN mtt.valor_cuota * mtt.numero_cuotas ELSE mtt.monto_transaccion END) monto_total, mtt.uso_de_clave :: INT :: BOOL clave, FALSE trx_relacionada, mtt.otorgamiento otorgamiento, mtt.numero_cuotas cuotas, mtt.valor_cuota valor_cuota, mtt.meses_diferimiento diferido, mtt.tasa_interes_cuotas tasa_interes, mtt.codigo_autorizacion codigo_autorizacion, mtt.fecha_transaccion fecha_ord FROM admmcta.mcta_transaccion_tarjeta mtt INNER JOIN admmref.mref_grupo_transaccion mgt ON mgt.id = mtt.id_grupo_trx INNER JOIN admmcta.mcta_tipo_transaccion_tarjeta mttt ON mttt.id = mtt.id_tipo_transaccion_tj INNER JOIN admmcta.mcta_maestro_tarjeta mmt ON mtt.id_tarjeta = mmt.id_tarjeta INNER JOIN admmcta.mcta_marca_tarjeta mmarcat ON mmarcat.id = mmt.id_marca_tarjeta INNER JOIN admmref.mref_logo mrlogo ON mrlogo.id = mmarcat.id_logo INNER JOIN admmref.mref_tipo_rol_cliente mtrc ON mtrc.id = mmt.id_tipo_tarjeta WHERE 1 = 1 AND mttt.registro_seleccionable = 1 AND id_cliente = :idCliente AND mtt.fecha_transaccion >= NOW() - INTERVAL '25' MONTH AND mtt.codigo_autorizacion = '0') AS trx_tarjeta ORDER BY trx_tarjeta.fecha_ord, codigo_autorizacion DESC, (CASE WHEN cod_familia IN ('30', '180', '3000') THEN 1 ELSE 0 END) DESC, trx_tarjeta.id DESC",
                params,
                TrxVo.class)
                .collectList().block();

        Assert.assertTrue(trxs != null);

    }


    public static class TrxVo {
        private Integer id;
        private String familia;
        private String marca;
        private LocalDate fechaTrx;
        private LocalDate fechaPosteo;
        private java.lang.String tipoTitularidad;
        private java.lang.String tarjeta;
        private java.lang.String tipoTrx;
        private java.lang.String estado;
        private java.lang.String comercio;
        private java.lang.String origen;
        private BigDecimal montoTrx;
        private BigDecimal montoPie;
        private BigDecimal montoTotal;
        private java.lang.Boolean clave;
        private java.lang.Boolean trxRelacionada;
        private java.lang.String otorgamiento;
        private BigDecimal cuotas;
        private BigDecimal valorCuota;
        private BigDecimal diferido;
        private BigDecimal tasaInteres;
        private java.lang.String codigoAutorizacion;
        private java.lang.String simboloMonto;
        private LocalDate fechaOrdenamiento1;
    }

    public static class TestVO {
        String entidad;
        Integer idTipoCliente;
    }
}
