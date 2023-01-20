sam build
sam local invoke -e events/event.json -n env.json || exit 1
sam local invoke -e events/event-manifest.json -n env.json || exit 1
sam local invoke -e events/event-payload.json -n env.json || exit 1
