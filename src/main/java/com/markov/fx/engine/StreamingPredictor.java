package com.markov.fx.engine;

import com.markov.fx.model.Bar;
import com.markov.fx.model.OnlineMarkovRegimeModel;
import com.markov.fx.model.Prediction;

import java.util.Optional;

public class StreamingPredictor {
    private final OnlineMarkovRegimeModel model;

    public StreamingPredictor(OnlineMarkovRegimeModel model) {
        this.model = model;
    }

    public Optional<Prediction> onBar(Bar bar) {
        model.observeBar(bar);
        return model.predictNext(bar.timestamp());
    }
}
