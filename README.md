## graph_database_for_cbioportal
This repository contains the script used to make a graph database of cbioportal data.
The contents of this repository are 100% open source.


## Table of Contents
* [Intallation](#installation)
* [Usage](#usage)
* [Contributing](#contributing)
* [Credits](#credits)
* [License](#license)


## <a name="installation">Installation</a>
### Requirements

* setting connection information
  * Open the main script: "insert_cbioportal_data_into_graphDB.py"
  * Edit azure ID, DB name, TABLE name, and KEY_VALUe in the 'connect_server' function.

<br>

[![usage](https://github.com/hubert-bioinformatics/graph_database_for_cbioportal/blob/master/README_images/azure.png)](https://github.com/hubert-bioinformatics/graph_database_for_cbioportal/blob/master/README_images/azure.png)


## <a name="usage">Usage</a>

* Basic
  * The script parses a bunch of data from cbioportal data file,
  * and makes a graph database using azure cosmos DB.
  * The output is a graph database in azure cosmos DB.

* demo play
<br>

[![usage](https://github.com/hubert-bioinformatics/graph_database_for_cbioportal/blob/master/README_images/graph2.gif)](https://github.com/hubert-bioinformatics/graph_database_for_cbioportal/blob/master/README_images/graph2.gif)


## <a name="contributing">Contributing</a>


Welcome all contributions that can be a issue report or a pull request to the repository.


## <a name="credits">Credits</a>


hubert (Jong-Hyuk Kim)


## <a name="license">License</a>

Licensed under the MIT License.

