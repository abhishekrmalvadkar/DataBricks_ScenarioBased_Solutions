# Databricks notebook source
dbutils.fs.mount(source = 'wasbs://movies-input@demostorageabhishek.blob.core.windows.net',
                mount_point = '/mnt/blobStorage2',
                extra_configs = {'fs.azure.sas.movies-input.demostorageabhishek.blob.core.windows.net': dbutils.secrets.get('secretScopeName','secretName')} )
