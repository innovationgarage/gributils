"""
Usage samples:
gributils index --database="$DATABASE" add-file --filepath="/home/saghar/IG/projects/gributils/data/smhi/arome/AM25H2_201808300600+000H00M.grib"
gributils index --database="$DATABASE" add-dir --basedir="/home/saghar/IG/projects/gributils/data/smhi/arome" 1>&2
gributils index --database="$DATABASE" lookup layers --parameter-name="Temperature" --timestamp="2018-08-30 00:04:00" 
gributils index --database="$DATABASE" lookup layers --parameter-name="P Pressure" --timestamp="2018-08-29 00:30:00" --timestamp-last-before 1 --lat 58.496206 --lon 10.2360331
gributils index --database="$DATABASE" interp-latlon --gribfile "/home/saghar/IG/projects/gributils/data/smhi/arome/AM25H2_201808300600+000H00M.grib" --layeridx 13 --lat 60. --lon 0.
gributils index --database="$DATABASE" interp-timestamp --parameter-name="Temperature" --timestamp "2018-09-12 08:00:00" --lat 60 --lon 30
"""

import click
import click_datetime
import gributils.gribindex
import gributils.server
import json

@click.group()
@click.pass_context
def main(ctx, **kw):
    ctx.obj = {}

@main.command()
@click.option('--database', default="http://localhost:9200")
@click.option('--filearea', default=".")
@click.option('--host', default="0.0.0.0")
@click.option('--port', default=1028)
@click.pass_context
def server(ctx, database, filearea, host, port, **kw):
    gributils.server.filearea = filearea
    gributils.server.index = gributils.gribindex.GribIndex(database)
    gributils.server.app.run(host=host, port=port)
    
@main.group()
@click.option('--database')
@click.pass_context
def index(ctx, database, **kw):
    ctx.obj = {}
    ctx.obj['index'] = gributils.gribindex.GribIndex(database)

@index.command()
@click.pass_context
def initialize(ctx, **kw):
    ctx.obj["index"].init_db(**kw)

@index.command()
@click.pass_context
def initialize(ctx, **kw):
    ctx.obj["index"].init_db(**kw)

@index.command()
@click.argument("output", type=click.Choice(['layers', 'names', 'units', 'level-types', 'levels']))
@click.option('--timestamp', type=click_datetime.Datetime(format='%Y-%m-%dT%H:%M:%S.%fZ'), default=None)
@click.option('--timestamp-last-before', type=int)
@click.option('--parameter-name')
@click.option('--parameter-unit')
@click.option('--type-of-level')
@click.option('--level', type=float)
@click.option('--level-highest-below', is_flag=True)
@click.option('--lat', type=float)
@click.option('--lon', type=float)
@click.option('--pretty', is_flag=True)
@click.pass_context
def lookup(ctx, **kw):
    pretty = kw.pop("pretty", False)
    res = ctx.obj["index"].lookup(**kw)
    if pretty:
        print(json.dumps(res, indent=2))
    else:
        for row in res:
            print(json.dumps(row))
    
    # for result in ctx.obj["index"].lookup(**kw):
    #     print(result)
    
@index.command()
@click.option('--pretty', is_flag=True)
@click.pass_context
def bboxes(ctx, **kw):
    pretty = kw.pop("pretty", False)
    res = ctx.obj["index"].get_grid_bboxes(**kw)
    if pretty:
        print(json.dumps(res, indent=2))
    else:
        for row in res:
            print(json.dumps(row))

@index.command()
@click.option('--lat', type=float)
@click.option('--lon', type=float)
@click.option('--pretty', is_flag=True)
@click.pass_context
def grids(ctx, **kw):
    pretty = kw.pop("pretty", False)
    res = ctx.obj["index"].get_grids_for_position(**kw)
    if pretty:
        print(json.dumps(res, indent=2))
    else:
        for row in res:
            print(json.dumps(row))

@index.command()
@click.option('--gribfile', type=str)
@click.option('--layeridx', type=str)
@click.option('--lat', type=float)
@click.option('--lon', type=float)
@click.pass_context
def interp_latlon(ctx, **kw):
    print(ctx.obj["index"].interp_latlon(**kw))

@index.command()
@click.option('--timestamp', type=click_datetime.Datetime(format='%Y-%m-%d %H:%M:%S'), default=None)
@click.option('--parameter-name')
@click.option('--parameter-unit')
@click.option('--type-of-level')
@click.option('--level', type=float)
@click.option('--level-highest-below', is_flag=True)
@click.option('--lat', type=float)
@click.option('--lon', type=float)
@click.option('--pretty', is_flag=True)
@click.pass_context
def interp_timestamp(ctx, **kw):
    pretty = kw.pop("pretty", False)
    res = ctx.obj["index"].interp_timestamp(**kw)
    if pretty:
        print(json.dumps(res, indent=2))
    else:
        for row in res:
            print(json.dumps(row))
            
@index.command()
@click.option("--filepath", type=str)
@click.option("--parametermap", type=str)
@click.pass_context
def add_file(ctx, **kw):
    ctx.obj["index"].add_file(**kw)

def show_error(err):
    print(err)
    
@index.command()
@click.option("--basedir", type=str)
@click.option("--parametermap", type=str)
@click.pass_context
def add_dir(ctx, **kw):
    ctx.obj["index"].add_dir(**kw, cb=show_error)
    
@index.group()
@click.pass_context
def parametermap(ctx, **kw):
    pass

@parametermap.command()
@click.option("--name", type=str)
@click.option("--mapping", type=str)
@click.pass_context
def add(ctx, **kw):
    ctx.obj["index"].add_parametermap(**kw)

@parametermap.command()
@click.pass_context
def list(ctx, **kw):
    for name in ctx.obj["index"].get_parametermaps(**kw):
        print(name)


