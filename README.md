This is a rewrite in Java from the Playframework Liquibase plugin
from https://github.com/mknittig/play-liquibase that was written in
Scala.

This plugin does the following things, as it tries to combine the robustness of Liquibase and the implementation of
logging database changes the way Evolution does.

On start (of the application) it first checks if the CONTENTCHANGELOG table exists. If not,create it.

Next, it checks if all changelog files (containing the Liquibase change sets) that are defined in the
DATABASECHANGELOG table are available.
If not, there probably is a checkout of older code from Github, so it reacts as following:

A: Regenerate the changelog file as it's stored in the CONTENTCHANGELOG table

B: Generate a temp master file for Liquibase

C: Rollback the last change

D: Clean up the master temp file and regenerated changelog file

E: Clean up the database ( Delete the corresponding row in the CONTENTCHANGELOG table ).

Do this for all found changelog items. (it iterates sorted by ORDEREXECUTED)

Next check all available changelogs to see if there are changes made to it, and if so, rollback the old change, so
we can apply the next (new) change.


Now check if there are changes in the database that Liquibase shoud apply

Test if Liquibase is able to apply them

Apply the changes to the database

Store the changes made from the changelog file in our CONTENTCHANGELOG table

Licence
This source code is released under the Apache 2 licence.
