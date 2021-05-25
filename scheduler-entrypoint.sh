#!/bin/bash

celery beat -l DEBUG -A gwvolman.scheduler -b redis://redis:6379/  --result-backend redis://redis:6379/
