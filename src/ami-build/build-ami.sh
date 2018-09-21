#!/usr/bin/env bash

if ! which packer > /dev/null; then
    echo "Install packer"
    exit 1
fi

template='cat "./packer_template.json"'
build_start_time="$(date +'%s')"

packer build <(eval "$template") -machine-readable \
    | tee "build.log" \
        >(
            awk -F "," '{
                if (($3 == "artifact") && ($5 == "id")) {
                    print $0
                }
            }' > "output.csv"
        )

ami_count=$(
    awk -F "," '{
        split($6, artifact_ids, "\\%\\!\\(PACKER\\_COMMA\\)")

        for (i in artifact_ids) {
            print artifact_ids[i]
        }
    }' "./output.csv" | wc -l | tr -d ' '
)

echo ""
echo "Successfully registered $ami_count AMIs."

popd > /dev/null

build_end_time="$(date +'%s')"

diff_secs="$(($build_end_time-$build_start_time))"
build_mins="$(($diff_secs / 60))"
build_secs="$(($diff_secs - $build_mins * 60))"

echo ""
echo "Build completed successfully in: ${build_mins}m ${build_secs}s."