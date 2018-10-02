import click
import click_datetime
import gributils.gribindex

@click.group()
@click.pass_context
def main(ctx, **kw):
    ctx.obj = {}

@main.group()
@click.option('--database')
@click.pass_context
def index(ctx, database, **kw):
    ctx.obj = {}
    ctx.obj['index'] = gributils.gribindex.GribIndex(database)
    
@index.command()
@click.argument("output", type=click.Choice(['layers', 'names', 'units', 'level-types', 'levels']))
@click.option('--timestamp', type=click_datetime.Datetime(format='%Y-%m-%d %H:%M:%S'), default=None)
@click.option('--timestamp-last-before', is_flag=True)
@click.option('--parameter-name')
@click.option('--parameter-unit')
@click.option('--type-of-level')
@click.option('--level', type=float)
@click.option('--level-highest-below', is_flag=True)
@click.option('--lat', type=float)
@click.option('--lon', type=float)
@click.pass_context
def lookup(ctx, **kw):
    def mangle(item):
        if hasattr(item, 'strftime'):
            return item.strftime("%Y-%m-%d %H:%M:%S")
        return str(item)
    for result in ctx.obj["index"].lookup(**kw):
        print(", ".join(mangle(item) for item in result))

@index.command()
@click.option("--filepath", type=str)
@click.pass_context
def add_file(ctx, **kw):
    def mangle(item):
        if hasattr(item, 'strftime'):
            return item.strftime("%Y-%m-%d %H:%M:%S")
        return str(item)
    ctx.obj["index"].add_file(**kw)

def show_error(err):
    print(err)
    
@index.command()
@click.option("--basedir", type=str)
@click.pass_context
def add_dir(ctx, **kw):
    def mangle(item):
        if hasattr(item, 'strftime'):
            return item.strftime("%Y-%m-%d %H:%M:%S")
        return str(item)
    ctx.obj["index"].add_dir(**kw, cb=show_error)
    
