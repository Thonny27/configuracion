-- VIAP - Creacion de data para envío de correo de la fase 3
declare @usuarioCreacion varchar(4) = '9000'

declare @colaimpresion as table (
	Id int identity,
	NumeroImpresion int primary key,
	NumeroCarta smallint,
	CodigoTipoSolicitudSC smallint,
	NumeroSolicitudSC int,
	CodigoDocumento int,
	CodigoAgencia smallint,
	FechaCreacion datetime,
	UsuarioCreacion	smallint
)

;with colaimpresion as (
	select
	ROW_NUMBER() OVER(PARTITION BY bi.CodigoDocumento ORDER BY bi.FechaEmision desc) Fila, 
	bi.NumeroImpresion, bi.NumeroCarta, bi.CodigoAgencia, bi.CodigoDocumento, bi.NumeroSolicitudSC, bi.CodigoTipoSolicitudSC, bi.UsuarioEmision
	from BITACORA_IMPRESION bi with(nolock)
	inner join (
		select distinct a.NumeroImpresion
		from (
			select distinct NumeroImpresion, count(1) Cantidad
			from DETALLE_BITACORA_IMPRESION with(nolock)
			where Parametro in ('&CodigoSBS&', '&FechaEmision&', '&FechaInicioVigenciaPoliza&', '&FechaFinVigenciaPoliza&', '&AniosVigenciaContrato&', '&MonedaSimbolo&', '&MonedaContratoLetras&', '&MonedaComisionSimbolo&', '&SumaAsegurada&', '&PorcentajeRetorno12Anios&', '&Agencia&', '&Campaña&', '&Departamento&', '&CodigoDepartamento&', '&Sexo&', '&NombreProducto_Desc&', '&Estado&', '&CodigoVia&', '&NombreVia&', '&Manzana&', '&Lote&', '&Interior&', '&Referencia&', '&Numero&', '&CodigoAgrupamiento&', '&NombreAgrupamiento&', '&Provincia&', '&Distrito&', '&CodigoModalidad&', '&ApellidoMaternoContratante&', '&NombreContratante&', '&NombreComercial&', '&TipoPersona&', '&TelefonoDia&', '&TelefonoMovil&', '&TelefonoNoche&', '&TelefonoFax&', '&CodigoProducto&', '&BeneficiarioPrincipal1_Nombre&', '&BeneficiarioPrincipal1_Parentesco&', '&BeneficiarioPrincipal1_Porcentaje&', '&BeneficiarioPrincipal2_Nombre&', '&BeneficiarioPrincipal2_Parentesco&', '&BeneficiarioPrincipal2_Porcentaje&', '&BeneficiarioPrincipal3_Nombre&', '&BeneficiarioPrincipal3_Parentesco&', '&BeneficiarioPrincipal3_Porcentaje&', '&BeneficiarioPrincipal4_Nombre&', '&BeneficiarioPrincipal4_Parentesco&', '&BeneficiarioPrincipal4_Porcentaje&', '&BeneficiarioPrincipal5_Nombre&', '&BeneficiarioPrincipal5_Parentesco&', '&BeneficiarioPrincipal5_Porcentaje&', '&BeneficiarioPrincipal6_Nombre&', '&BeneficiarioPrincipal6_Parentesco&', '&BeneficiarioPrincipal6_Porcentaje&', '&NumeroSolicitud&', '&CorrelativoPoliza&', '&UrlWeb&', '&EdadmaximaContratacion&', '&CargoFuncionario2&', '&NombreFuncionario2&', '&CargoFuncionario1&', '&NombreFuncionario1&', '&DireccionCorrespondenciaLinea3&', '&DireccionCorrespondenciaLinea2&', '&DireccionCorrespondenciaLinea1&', '&EmailContratante&', '&DireccionContratante&', '&NroDocumentoIdentidadContratante&', '&TipoDocumentoIdentidadContratante&', '&SufijoSexo&', '&ApellidoPaternoContratante&', '&PrimerNombreCliente&', '&Vocativo&', '&NombreRazonSocialContratante&', '&Tratamiento&', '&TelefonoCentralAseguradora&', '&RazonSocialLargaAseguradora&', '&RazonSocialCortaAseguradora&', '&Direccion2&', '&Direccion1&', '&NombreProducto&', '&Firma2_Linea3&', '&Firma1_Linea3&', '&NumeroMedioPago&', '&DireccionCompleta&', '&ConsentimientoCliente&', '&NumRegistroComercializador&', '&ImporteComisionComercializador&', '&TelefonoComercializador&', '&NombreComercializador&', '&TCEAAnual&', '&TCEASemestral&', '&TCEAMensual&', '&LugarPago&', '&FormaPago&', '&FrecuenciaPago&', '&ImportePrimaTotal&', '&ImporteIGV&', '&ImportePrimaComercial&', '&EmailAsegurado&', '&DireccionAsegurado&', '&EdadContratoAsegurado&', '&FechaNacimientoAsegurado&', '&NroDocumentoIdentidadAsegurado&', '&TipoDocumentoIdentidadAsegurado&', '&NombreRazonSocialAsegurado&', '&PlanCobertura&', '&PorcentajeRetorno12FinVigencia&', '&NumeroPoliza&', '&NumeroPolizaFormato&')
			group by NumeroImpresion
			having count(1) = 112
		) a
	) t on bi.NumeroImpresion = t.NumeroImpresion
	left join COLA_IMPRESION ci on bi.CodigoDocumento = ci.CodigoDocumento
	where ci.NumeroImpresion is null
	and bi.CodigoDocumento is not null
	and bi.NumeroSolicitudSC is not null
	and bi.CodigoTipoSolicitudSC is not null
	and bi.NumeroCarta in (940)
)

insert into @colaimpresion
(NumeroImpresion, NumeroCarta, CodigoTipoSolicitudSC, NumeroSolicitudSC, CodigoDocumento, CodigoAgencia, FechaCreacion, UsuarioCreacion)
select top 10
NumeroImpresion, NumeroCarta, CodigoTipoSolicitudSC, NumeroSolicitudSC, CodigoDocumento, CodigoAgencia, getdate(), @usuarioCreacion UsuarioEmision
from colaimpresion
where Fila = 1

select ci.NumeroCarta, ci.CodigoTipoSolicitudSC, ci.NumeroSolicitudSC, ci.CodigoDocumento, ci.CodigoAgencia, ci.FechaCreacion, ci.UsuarioCreacion, ci.FechaCreacion, ci.UsuarioCreacion, cast(ci.NumeroImpresion as varchar(255)) from @colaimpresion ci

declare @colaimpresioninserteds as table (NumeroImpresionNew int, NumeroImpresionOld int)
  
insert into COLA_IMPRESION
(NumeroCarta, CodigoTipoSolicitudSC, NumeroSolicitudSC, CodigoDocumento, CodigoAgencia, FechaCreacion, UsuarioCreacion, FechaUltimaModificacion, UsuarioUltimaModificacion, IDFilenet)
output inserted.NumeroImpresion, cast(inserted.IDFilenet as int) into @colaimpresioninserteds
select 
ci.NumeroCarta, ci.CodigoTipoSolicitudSC, ci.NumeroSolicitudSC, ci.CodigoDocumento, ci.CodigoAgencia, ci.FechaCreacion, ci.UsuarioCreacion, ci.FechaCreacion, ci.UsuarioCreacion, cast(ci.NumeroImpresion as varchar(255))
from @colaimpresion ci

select * from @colaimpresioninserteds

insert into DETALLE_COLA_IMPRESION
(NumeroImpresion, NumeroDetalle, Parametro, Valor, TipoParametro)
select cii.NumeroImpresionNew, dbi.NumeroDetalle, dbi.Parametro, dbi.Valor, dbi.TipoParametro
from DETALLE_BITACORA_IMPRESION dbi
inner join @colaimpresioninserteds cii on dbi.NumeroImpresion = cii.NumeroImpresionOld

update dci set
Valor = 'ronald.seancas@teamsoft.com.pe'
from DETALLE_COLA_IMPRESION dci
inner join @colaimpresioninserteds cii on dci.NumeroImpresion = cii.NumeroImpresionNew
where dci.Parametro in ('&EmailAsegurado&', '&EmailContratante&')
and nullif(trim(Valor), '') is not null

update dci set Valor = '4'
from COLA_IMPRESION ci
inner join DETALLE_COLA_IMPRESION dci on ci.NumeroImpresion = dci.NumeroImpresion
inner join @colaimpresioninserteds cii on dci.NumeroImpresion = cii.NumeroImpresionNew
where dci.Parametro = '&CodigoModalidad&'
and nullif(trim(Valor), '') is not null
and Valor != '4'
