import os


def resources_folder(file_object, file_name) -> str:
    """
    Purposely avoiding "test" filename or "test" function name for this file and function(s) so pytest will not grab it.
    Based on the input file object, produce a correct absolute file path as a string. This assists with finding
    comparison resources that we have stored in a folder called "resources" below the folder that contains the test.
    @param file_object  A file object obtained with __file__ in the calling test function.
    @param file_name  The name of the file in the folder called "resources" below the folder that contains the test.
    @return str
    """
    base_dir = os.path.dirname(os.path.abspath(file_object))
    file_path = os.path.join(base_dir, "resources", file_name)
    return file_path
