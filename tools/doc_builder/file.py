import ast
import glob
from format_docstring import create_md_content


def get_files(root):
    ob = glob.iglob(root + '/**.py', recursive=True)
    ob = [o for o in ob if not o.endswith('__init__.py')]
    ob = [o for o in ob if not '/internals/' in o]
    return ob

def process_file(filepath, title):

    file_contents = ""
    with open(filepath) as fd:
        file_contents = fd.read()

    if '#nodoc' in file_contents:
        return

    module = ast.parse(file_contents)

    cache = '# ' + title + '\n'

    for node in ast.iter_child_nodes(module):

        if isinstance(node, ast.ClassDef):
            #cache += create_md_content(node, title=node.name)
            class_name = node.name
            child_nodes = list(ast.iter_child_nodes(node))

            child_nodes = [c for c in child_nodes if hasattr(c, 'name')]

            init = [child_node for child_node in child_nodes if child_node.name == "__init__"]
            if len(init) > 0:
                init = init.pop()
                cache += create_md_content(init, title=node.name)

            for child_node in [child_node for child_node in child_nodes if not child_node.name.startswith('_')]:
                print('method:', child_node.name)
                if isinstance(child_node, ast.FunctionDef): 
                    cache += create_md_content(child_node, title=child_node.name)

        if isinstance(node, ast.FunctionDef):
            cache += create_md_content(node, title=node.name)

    return cache

file_list = get_files('./orwell/**')
for module in file_list:
    print('module:', module)
    save = module
    save = save.removeprefix('./orwell/')
    save = save.removesuffix('.py')
    save = save.replace('/', '.')
    doc = process_file(module, 'orwell.' + save)
    save = 'docs/orwell.' + save + '.md'
    if doc is not None:
        open(save, 'w').write(doc)