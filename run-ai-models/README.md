
## Download and save models
* Downloaded the models from Google Drive
* Saved the models in the `models` directory under the project 
* Unzip'ed the files

models
  comparison_transfer.model.zip  
  comparison_v1.model.zip  
  universal-sentence-encoder-large_5.zip

Unzip'ed files
  comparison_transfer.model
  comparison_v1.model
  universal-sentence-encoder-large_5

## PIP Install AI packages

`pipenv install tensorflow`

Had error:
pipenv.patched.notpip._internal.exceptions.InstallationError: Command "git checkout -q amz2_branch" failed with error code 1 in /tmp/requirementslib4b6mob0v/zed-event

Commented out:
zed-event = {git = "https://github.com/cdlib/zephir-services.git",ref = "amz2_branch",subdirectory = "zed-event"}

`pipenv install tensorflow`
`pipenv install tensorflow-hub`

Note: packages installation succeeded. however, pipenv failed at Locking [packages] dependencies… 

Packages installed (pip list):
keras                   2.7.0rc0
Keras-Preprocessing     1.1.2

tensorflow-estimator    2.6.0
tensorflow-hub          0.12.0

Quesiton: how come the tensorflow pacakge is not listed?

## reinstalled the packages outside to pipenv
pip3 install tensorflow --user
pip3 install tensorflow-hub --user
