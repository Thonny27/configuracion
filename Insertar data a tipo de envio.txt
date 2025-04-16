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
			where Parametro in ('&EmailDestinatario&','&EmailCopia&','&EmailCopiaOculta&','&RutaArchivo&','&NombreArchivo&','&NombreEnvio&','&TipoDocumento&','&NumeroDocumento&','&DatoOpcional&','&Saludo&','&NombreProducto&','&NumeroPoliza&','&MotivoRechazo&','&texto1&')
			group by NumeroImpresion
			having count(1) = 15 -- este valor es variable dependiendo la cantidad de parametros que tenemos y que usaremos en Parametro in()
		) a
	) t on bi.NumeroImpresion = t.NumeroImpresion
	left join COLA_IMPRESION ci on bi.CodigoDocumento = ci.CodigoDocumento
	where 
	--ci.NumeroImpresion is null
	--and
	 bi.CodigoDocumento is not null
	and bi.NumeroSolicitudSC is not null
	and bi.CodigoTipoSolicitudSC is not null
	and bi.NumeroCarta in (963)
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
Valor = 'anthony.alarcon@teamsoft.com.pe'
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

