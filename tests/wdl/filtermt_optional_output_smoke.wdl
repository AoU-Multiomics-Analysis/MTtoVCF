version 1.0

workflow FilterMTOptionalOutputSmoke {
    input {
        Boolean AnnotateWithVAT = false
    }

    call EmitOptionalOutput {
        input:
            AnnotateWithVAT = AnnotateWithVAT
    }

    output {
        File? TranscriptAnnotations = EmitOptionalOutput.TranscriptAnnotations
    }
}

task EmitOptionalOutput {
    input {
        Boolean AnnotateWithVAT
    }

    command <<<
        true
    >>>

    runtime {
        docker: "hailgenetics/hail:0.2.134-py3.11"
    }

    output {
        File? TranscriptAnnotations = if AnnotateWithVAT then read_string('transcript_annotations_outpath.txt') else 'transcript_annotations_outpath.txt'
    }
}
